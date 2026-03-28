// Package server provides the Bleve-backed storage handler for SlideBolt.
// It indexes all stored documents for full-text and structured field queries.
// Used by the sb-storage binary (production) and manager-sdk TestEnv (testing).
package server

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search/searcher"
	"github.com/fsnotify/fsnotify"

	messenger "github.com/slidebolt/sb-messenger-sdk"
	storage "github.com/slidebolt/sb-storage-sdk"
)

func init() {
	// Allow unlimited disjunction clauses for wildcard-heavy queries.
	searcher.DisjunctionMaxClauseCount = 0
}

// userFields are the JSON keys that belong in the sidecar (.profile.json),
// not in the main entity file. Save() strips these; SetProfile() writes them.
var userFields = []string{"labels", "meta", "profile"}

// Handler is the Bleve-backed storage engine. It stores raw JSON in an
// in-memory map for fast point reads and indexes documents in Bleve
// for structured queries.
//
// If dataDir is non-empty, all writes are persisted immediately to disk
// (write-through) and a file watcher syncs external edits back into memory.
//
// User-owned fields (labels, meta, profile) are stored in a separate
// .profile.json sidecar file. Save() never writes these fields;
// SetProfile() is the only write path. Get/Query/Search return merged data.
type Handler struct {
	mu       sync.RWMutex
	state    map[string]json.RawMessage // raw main json only
	data     map[string]json.RawMessage // merged view (entity + sidecar)
	sidecar  map[string]json.RawMessage // user-owned fields only
	private  map[string]json.RawMessage // plugin-owned durable private data
	internal map[string]json.RawMessage // plugin-owned opaque sidecar data
	source   map[string]string          // sb-script Lua source sidecar data
	index    bleve.Index
	dataDir  string
	msg      messenger.Messenger // for publishing state change events
}

// NewHandler creates an in-memory-only handler (no persistence).
func NewHandler() (*Handler, error) {
	return NewHandlerWithDir("")
}

// NewHandlerWithDir creates a handler backed by dataDir for persistence.
// Pass an empty string for in-memory-only (used in tests).
func NewHandlerWithDir(dataDir string) (*Handler, error) {
	mapping := bleve.NewIndexMapping()
	idx, err := bleve.NewMemOnly(mapping)
	if err != nil {
		return nil, fmt.Errorf("storage-server: bleve: %w", err)
	}
	return &Handler{
		state:    make(map[string]json.RawMessage),
		data:     make(map[string]json.RawMessage),
		sidecar:  make(map[string]json.RawMessage),
		private:  make(map[string]json.RawMessage),
		internal: make(map[string]json.RawMessage),
		source:   make(map[string]string),
		index:    idx,
		dataDir:  dataDir,
	}, nil
}

// Register subscribes the handler to storage.> on the given messenger.
// The messenger is also used to publish state.changed.{key} events when
// non-internal state is saved.
func (h *Handler) Register(msg messenger.Messenger) error {
	h.msg = msg
	_, err := msg.Subscribe("storage.>", func(m *messenger.Message) {
		h.handle(m)
	})
	return err
}

// Close closes the Bleve index.
func (h *Handler) Close() error {
	return h.index.Close()
}

// LoadFromDir walks dataDir and loads all JSON files into memory.
// Sidecar files (.profile.json) are loaded separately and merged.
// On first load, existing entities with labels/meta/profile in the main
// file are auto-migrated: those fields are extracted into a new sidecar.
func (h *Handler) LoadFromDir() (int, error) {
	if h.dataDir == "" {
		return 0, nil
	}
	store := make(map[string]json.RawMessage)
	sidecars := make(map[string]json.RawMessage)
	privates := make(map[string]json.RawMessage)
	internals := make(map[string]json.RawMessage)
	sources := make(map[string]string)
	err := filepath.Walk(h.dataDir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		if isLuaSourcePath(p) {
			key := h.luaSourcePathToKey(p)
			if key != "" {
				sources[key] = string(data)
			}
		} else if filepath.Ext(p) != ".json" {
			return nil
		} else if isSidecarPath(p) {
			key := h.sidecarPathToKey(p)
			if key != "" {
				sidecars[key] = json.RawMessage(data)
			}
		} else if isPrivatePath(p) {
			key := h.privatePathToKey(p)
			if key != "" {
				privates[key] = json.RawMessage(data)
			}
		} else if isInternalPath(p) {
			key := h.internalPathToKey(p)
			if key != "" {
				internals[key] = json.RawMessage(data)
			}
		} else {
			key := h.pathToKey(p)
			if key != "" {
				store[key] = json.RawMessage(data)
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if len(store) == 0 && len(sidecars) == 0 && len(privates) == 0 && len(internals) == 0 && len(sources) == 0 {
		return 0, os.ErrNotExist
	}

	// Auto-migrate: if a main file has user fields and no sidecar exists,
	// extract them into a sidecar file on disk.
	for key, data := range store {
		if _, hasSidecar := sidecars[key]; hasSidecar {
			continue
		}
		extracted := extractUserFields(data)
		if extracted != nil {
			sidecars[key] = extracted
			store[key] = stripUserFields(data)
			h.writeSidecarFile(key, extracted)
			h.writeFile(key, store[key])
			slog.Info("storage: migrated user fields to sidecar", "key", key)
		}
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	for k, v := range sidecars {
		h.sidecar[k] = v
	}
	for k, v := range store {
		h.state[k] = v
	}
	for k, v := range privates {
		h.private[k] = v
	}
	for k, v := range internals {
		h.internal[k] = v
	}
	for k, v := range sources {
		h.source[k] = v
	}
	for k, v := range store {
		merged := mergeWithSource(mergeWithSidecar(v, h.sidecar[k]), h.source[k])
		h.data[k] = merged
		var doc map[string]any
		if json.Unmarshal(merged, &doc) == nil {
			doc["_key"] = k
			h.index.Index(k, doc)
		}
	}
	return len(store), nil
}

// StartWatcher watches dataDir for external file changes and syncs them into memory.
func (h *Handler) StartWatcher() error {
	if h.dataDir == "" {
		return nil
	}
	if err := os.MkdirAll(h.dataDir, 0755); err != nil {
		return err
	}
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("storage: watcher: %w", err)
	}
	// Watch root and all existing subdirectories.
	filepath.Walk(h.dataDir, func(p string, info os.FileInfo, err error) error {
		if err == nil && info.IsDir() {
			watcher.Add(p)
		}
		return nil
	})
	go h.watchLoop(watcher)
	return nil
}

func (h *Handler) watchLoop(watcher *fsnotify.Watcher) {
	defer watcher.Close()

	var dmu sync.Mutex
	debounce := make(map[string]*time.Timer)

	trigger := func(p string, fn func()) {
		dmu.Lock()
		defer dmu.Unlock()
		if t, ok := debounce[p]; ok {
			t.Stop()
		}
		debounce[p] = time.AfterFunc(100*time.Millisecond, func() {
			dmu.Lock()
			delete(debounce, p)
			dmu.Unlock()
			fn()
		})
	}

	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			// Watch newly created subdirectories.
			if event.Has(fsnotify.Create) {
				if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
					watcher.Add(event.Name)
					continue
				}
			}
			if filepath.Ext(event.Name) != ".json" && filepath.Ext(event.Name) != ".lua" {
				continue
			}
			p := event.Name
			if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) {
				trigger(p, func() { h.fileUpdated(p) })
			}
			if event.Has(fsnotify.Remove) || event.Has(fsnotify.Rename) {
				trigger(p, func() { h.fileRemoved(p) })
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			slog.Warn("storage: watcher error", "err", err)
		}
	}
}

func (h *Handler) fileUpdated(p string) {
	data, err := os.ReadFile(p)
	if err != nil || !json.Valid(data) {
		if !isLuaSourcePath(p) {
			return
		}
	}

	if isLuaSourcePath(p) {
		key := h.luaSourcePathToKey(p)
		if key == "" {
			return
		}
		h.mu.Lock()
		h.source[key] = string(data)
		if existing, ok := h.state[key]; ok {
			merged := mergeWithSource(mergeWithSidecar(existing, h.sidecar[key]), string(data))
			h.data[key] = merged
			var doc map[string]any
			if json.Unmarshal(merged, &doc) == nil {
				doc["_key"] = key
				h.index.Index(key, doc)
			}
		}
		h.mu.Unlock()
		slog.Info("storage: lua source synced from disk", "key", key)
		return
	}

	if isSidecarPath(p) {
		key := h.sidecarPathToKey(p)
		if key == "" {
			return
		}
		h.mu.Lock()
		h.sidecar[key] = json.RawMessage(data)
		// Re-merge with existing entity data.
		if existing, ok := h.state[key]; ok {
			merged := mergeWithSource(mergeWithSidecar(existing, json.RawMessage(data)), h.source[key])
			h.data[key] = merged
			var doc map[string]any
			if json.Unmarshal(merged, &doc) == nil {
				doc["_key"] = key
				h.index.Index(key, doc)
			}
		}
		h.mu.Unlock()
		slog.Info("storage: sidecar synced from disk", "key", key)
		return
	}
	if isPrivatePath(p) {
		key := h.privatePathToKey(p)
		if key == "" {
			return
		}
		h.mu.Lock()
		h.private[key] = json.RawMessage(data)
		h.mu.Unlock()
		slog.Info("storage: private sidecar synced from disk", "key", key)
		return
	}
	if isInternalPath(p) {
		key := h.internalPathToKey(p)
		if key == "" {
			return
		}
		h.mu.Lock()
		h.internal[key] = json.RawMessage(data)
		h.mu.Unlock()
		slog.Info("storage: internal sidecar synced from disk", "key", key)
		return
	}

	key := h.pathToKey(p)
	if key == "" {
		return
	}
	raw := json.RawMessage(data)
	merged := mergeWithSource(mergeWithSidecar(raw, h.sidecar[key]), h.source[key])
	var doc map[string]any
	json.Unmarshal(merged, &doc)

	h.mu.Lock()
	h.state[key] = raw
	h.data[key] = merged
	if doc != nil {
		doc["_key"] = key
		h.index.Index(key, doc)
	}
	h.mu.Unlock()
	slog.Info("storage: file synced from disk", "key", key)
}

func (h *Handler) fileRemoved(p string) {
	if isLuaSourcePath(p) {
		key := h.luaSourcePathToKey(p)
		if key == "" {
			return
		}
		h.mu.Lock()
		delete(h.source, key)
		if existing, ok := h.data[key]; ok {
			stripped := stripSourceField(stripUserFields(existing))
			merged := mergeWithSidecar(stripped, h.sidecar[key])
			h.data[key] = merged
			var doc map[string]any
			if json.Unmarshal(merged, &doc) == nil {
				doc["_key"] = key
				h.index.Index(key, doc)
			}
		}
		h.mu.Unlock()
		slog.Info("storage: lua source removed from disk", "key", key)
		return
	}

	if isSidecarPath(p) {
		key := h.sidecarPathToKey(p)
		if key == "" {
			return
		}
		h.mu.Lock()
		delete(h.sidecar, key)
		// Re-merge: entity data without sidecar.
		if existing, ok := h.data[key]; ok {
			stripped := stripUserFields(existing)
			h.data[key] = stripped
			var doc map[string]any
			if json.Unmarshal(stripped, &doc) == nil {
				doc["_key"] = key
				h.index.Index(key, doc)
			}
		}
		h.mu.Unlock()
		slog.Info("storage: sidecar removed from disk", "key", key)
		return
	}
	if isPrivatePath(p) {
		key := h.privatePathToKey(p)
		if key == "" {
			return
		}
		h.mu.Lock()
		delete(h.private, key)
		h.mu.Unlock()
		slog.Info("storage: private sidecar removed from disk", "key", key)
		return
	}
	if isInternalPath(p) {
		key := h.internalPathToKey(p)
		if key == "" {
			return
		}
		h.mu.Lock()
		delete(h.internal, key)
		h.mu.Unlock()
		slog.Info("storage: internal sidecar removed from disk", "key", key)
		return
	}

	key := h.pathToKey(p)
	if key == "" {
		return
	}
	h.mu.Lock()
	delete(h.state, key)
	delete(h.data, key)
	h.index.Delete(key)
	h.mu.Unlock()
	slog.Info("storage: file removed from disk", "key", key)
}

// keyToPath converts "esphome.living_room.light_001" →
// "<dataDir>/esphome/living_room/light_001/light_001.json"
func (h *Handler) keyToPath(key string) string {
	return h.keyToNamedPath(key, ".json")
}

// keyToSidecarPath converts "esphome.living_room.light_001" →
// "<dataDir>/esphome/living_room/light_001/light_001.profile.json"
func (h *Handler) keyToSidecarPath(key string) string {
	return h.keyToNamedPath(key, ".profile.json")
}

// keyToPrivatePath converts "esphome.living_room.light_001" →
// "<dataDir>/esphome/living_room/light_001/light_001.private.json"
func (h *Handler) keyToPrivatePath(key string) string {
	return h.keyToNamedPath(key, ".private.json")
}

// keyToInternalPath converts "esphome.living_room.light_001" →
// "<dataDir>/esphome/living_room/light_001/light_001.internal.json"
func (h *Handler) keyToInternalPath(key string) string {
	return h.keyToNamedPath(key, ".internal.json")
}

// pathToKey is the inverse of keyToPath.
func (h *Handler) pathToKey(p string) string {
	return h.namedPathToKey(p, ".json")
}

// sidecarPathToKey converts a .profile.json path to an entity key.
func (h *Handler) sidecarPathToKey(p string) string {
	return h.namedPathToKey(p, ".profile.json")
}

// privatePathToKey converts a .private.json path to an entity key.
func (h *Handler) privatePathToKey(p string) string {
	return h.namedPathToKey(p, ".private.json")
}

// internalPathToKey converts a .internal.json path to a storage key.
func (h *Handler) internalPathToKey(p string) string {
	return h.namedPathToKey(p, ".internal.json")
}

func (h *Handler) keyToLuaSourcePath(key string) string {
	return h.keyToNamedPath(key, ".lua")
}

func (h *Handler) luaSourcePathToKey(p string) string {
	return h.namedPathToKey(p, ".lua")
}

func (h *Handler) keyToNamedPath(key, suffix string) string {
	parts := strings.Split(key, ".")
	elems := append([]string{h.dataDir}, parts...)
	return filepath.Join(filepath.Join(elems...), parts[len(parts)-1]+suffix)
}

func (h *Handler) namedPathToKey(p, suffix string) string {
	rel, err := filepath.Rel(h.dataDir, p)
	if err != nil {
		return ""
	}
	trimmed := strings.TrimSuffix(rel, suffix)
	parts := strings.Split(trimmed, string(filepath.Separator))
	if len(parts) < 2 || parts[len(parts)-1] != parts[len(parts)-2] {
		return ""
	}
	parts = parts[:len(parts)-1]
	return strings.Join(parts, ".")
}

// isSidecarPath returns true if the path is a .profile.json sidecar file.
func isSidecarPath(p string) bool {
	return strings.HasSuffix(p, ".profile.json")
}

// isPrivatePath returns true if the path is a .private.json sidecar file.
func isPrivatePath(p string) bool {
	return strings.HasSuffix(p, ".private.json")
}

// isInternalPath returns true if the path is a .internal.json sidecar file.
func isInternalPath(p string) bool {
	return strings.HasSuffix(p, ".internal.json")
}

func isLuaSourcePath(p string) bool {
	return strings.HasSuffix(p, ".lua")
}

// writeFile persists a key's value to disk immediately (write-through).
func (h *Handler) writeFile(key string, data json.RawMessage) {
	if h.dataDir == "" {
		return
	}
	p := h.keyToPath(key)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		slog.Warn("storage: mkdir failed", "key", key, "err", err)
		return
	}
	pretty, _ := json.MarshalIndent(data, "", "  ")
	if err := os.WriteFile(p, pretty, 0644); err != nil {
		slog.Warn("storage: write failed", "key", key, "err", err)
	}
}

func (h *Handler) writeLuaSourceFile(key, source string) {
	if h.dataDir == "" || source == "" {
		return
	}
	p := h.keyToLuaSourcePath(key)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		slog.Warn("storage: mkdir lua source failed", "key", key, "err", err)
		return
	}
	if err := os.WriteFile(p, []byte(source), 0644); err != nil {
		slog.Warn("storage: write lua source failed", "key", key, "err", err)
	}
}

// writeSidecarFile persists a sidecar to disk.
func (h *Handler) writeSidecarFile(key string, data json.RawMessage) {
	if h.dataDir == "" {
		return
	}
	p := h.keyToSidecarPath(key)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		slog.Warn("storage: mkdir sidecar failed", "key", key, "err", err)
		return
	}
	pretty, _ := json.MarshalIndent(data, "", "  ")
	if err := os.WriteFile(p, pretty, 0644); err != nil {
		slog.Warn("storage: write sidecar failed", "key", key, "err", err)
	}
}

// writePrivateFile persists a private sidecar to disk.
func (h *Handler) writePrivateFile(key string, data json.RawMessage) {
	if h.dataDir == "" {
		return
	}
	p := h.keyToPrivatePath(key)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		slog.Warn("storage: mkdir private failed", "key", key, "err", err)
		return
	}
	pretty, _ := json.MarshalIndent(data, "", "  ")
	if err := os.WriteFile(p, pretty, 0644); err != nil {
		slog.Warn("storage: write private failed", "key", key, "err", err)
	}
}

// writeInternalFile persists an internal sidecar to disk.
func (h *Handler) writeInternalFile(key string, data json.RawMessage) {
	if h.dataDir == "" {
		return
	}
	p := h.keyToInternalPath(key)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		slog.Warn("storage: mkdir internal failed", "key", key, "err", err)
		return
	}
	pretty, _ := json.MarshalIndent(data, "", "  ")
	if err := os.WriteFile(p, pretty, 0644); err != nil {
		slog.Warn("storage: write internal failed", "key", key, "err", err)
	}
}

// deleteFile removes a key's file from disk immediately.
func (h *Handler) deleteFile(key string) {
	if h.dataDir == "" {
		return
	}
	h.removeFileAndEmptyDir(h.keyToPath(key))
}

// deleteSidecarFile removes a key's sidecar file from disk.
func (h *Handler) deleteSidecarFile(key string) {
	if h.dataDir == "" {
		return
	}
	h.removeFileAndEmptyDir(h.keyToSidecarPath(key))
}

// deletePrivateFile removes a key's private sidecar from disk.
func (h *Handler) deletePrivateFile(key string) {
	if h.dataDir == "" {
		return
	}
	h.removeFileAndEmptyDir(h.keyToPrivatePath(key))
}

// deleteInternalFile removes a key's internal sidecar from disk.
func (h *Handler) deleteInternalFile(key string) {
	if h.dataDir == "" {
		return
	}
	h.removeFileAndEmptyDir(h.keyToInternalPath(key))
}

func (h *Handler) deleteLuaSourceFile(key string) {
	if h.dataDir == "" {
		return
	}
	h.removeFileAndEmptyDir(h.keyToLuaSourcePath(key))
}

func (h *Handler) removeFileAndEmptyDir(p string) {
	if err := os.Remove(p); err != nil {
		return
	}
	dir := filepath.Dir(p)
	if dir == "" || dir == "." || dir == h.dataDir {
		return
	}
	_ = os.Remove(dir)
}

func (h *Handler) handle(m *messenger.Message) {
	parts := strings.SplitN(m.Subject, ".", 2)
	if len(parts) < 2 {
		respond(m, response{Error: "invalid subject"})
		return
	}
	switch parts[1] {
	case "save":
		h.handleSave(m)
	case "get":
		h.handleGet(m)
	case "delete":
		h.handleDelete(m)
	case "search":
		h.handleSearch(m)
	case "query":
		h.handleQuery(m)
	case "setprofile":
		h.handleSetProfile(m)
	default:
		respond(m, response{Error: "unknown operation: " + parts[1]})
	}
}

func (h *Handler) handleSave(m *messenger.Message) {
	var req saveRequest
	if err := json.Unmarshal(m.Data, &req); err != nil {
		respond(m, response{Error: "bad request: " + err.Error()})
		return
	}

	if req.Target == storage.Private {
		if !isJSONObject(req.Data) {
			respond(m, response{Error: "private data must be a JSON object"})
			return
		}
		h.mu.Lock()
		h.private[req.Key] = req.Data
		h.mu.Unlock()
		h.writePrivateFile(req.Key, req.Data)
		respond(m, response{OK: true})
		return
	}

	if req.Target == storage.Internal {
		h.mu.Lock()
		h.internal[req.Key] = req.Data
		h.mu.Unlock()
		h.writeInternalFile(req.Key, req.Data)
		respond(m, response{OK: true})
		return
	}

	if req.Target == storage.Profile {
		h.mu.Lock()
		h.sidecar[req.Key] = req.Data
		if existing, ok := h.state[req.Key]; ok {
			merged := mergeWithSource(mergeWithSidecar(existing, req.Data), h.source[req.Key])
			h.data[req.Key] = merged
			var doc map[string]any
			if json.Unmarshal(merged, &doc) == nil {
				doc["_key"] = req.Key
				h.index.Index(req.Key, doc)
			}
		}
		h.mu.Unlock()
		h.writeSidecarFile(req.Key, req.Data)
		respond(m, response{OK: true})
		if h.msg != nil {
			h.mu.RLock()
			data, ok := h.data[req.Key]
			h.mu.RUnlock()
			if ok {
				h.msg.Publish("state.changed."+req.Key, data)
			}
		}
		return
	}

	if req.Target == storage.Source {
		var source string
		if err := json.Unmarshal(req.Data, &source); err != nil || source == "" {
			respond(m, response{Error: "source data must be a JSON string"})
			return
		}
		h.mu.Lock()
		h.source[req.Key] = source
		if existing, ok := h.state[req.Key]; ok {
			merged := mergeWithSource(mergeWithSidecar(existing, h.sidecar[req.Key]), source)
			h.data[req.Key] = merged
			var doc map[string]any
			if json.Unmarshal(merged, &doc) == nil {
				doc["_key"] = req.Key
				h.index.Index(req.Key, doc)
			}
		}
		h.mu.Unlock()
		h.writeLuaSourceFile(req.Key, source)
		respond(m, response{OK: true})
		if h.msg != nil {
			h.mu.RLock()
			data, ok := h.data[req.Key]
			h.mu.RUnlock()
			if ok {
				h.msg.Publish("state.changed."+req.Key, data)
			}
		}
		return
	}

	// For non-internal saves, strip user-owned fields from the data
	// and merge with the existing sidecar for the in-memory view.
	saveData := req.Data   // what goes to disk
	mergedData := req.Data // what goes to memory/index/events
	saveData = stripUserFields(req.Data)
	luaSource, hasLuaSource := extractLuaSource(req.Key, saveData)
	if hasLuaSource {
		saveData = stripSourceField(saveData)
	}

	h.mu.Lock()
	h.state[req.Key] = saveData
	if hasLuaSource {
		h.source[req.Key] = luaSource
	}
	mergedData = mergeWithSource(mergeWithSidecar(saveData, h.sidecar[req.Key]), h.source[req.Key])
	h.data[req.Key] = mergedData
	var doc map[string]any
	json.Unmarshal(mergedData, &doc)
	if doc != nil {
		doc["_key"] = req.Key
		h.index.Index(req.Key, doc)
	}
	h.mu.Unlock()

	h.writeFile(req.Key, saveData)
	if hasLuaSource {
		h.writeLuaSourceFile(req.Key, luaSource)
	}
	respond(m, response{OK: true})

	// Notify subscribers that this key's state changed.
	if h.msg != nil {
		h.msg.Publish("state.changed."+req.Key, mergedData)
	}
}

func (h *Handler) handleSetProfile(m *messenger.Message) {
	var req saveRequest
	if err := json.Unmarshal(m.Data, &req); err != nil {
		respond(m, response{Error: "bad request: " + err.Error()})
		return
	}

	key := req.Key

	h.mu.Lock()
	h.sidecar[key] = req.Data
	// Re-merge with existing entity data if present.
	if existing, ok := h.state[key]; ok {
		merged := mergeWithSource(mergeWithSidecar(existing, req.Data), h.source[key])
		h.data[key] = merged
		var doc map[string]any
		if json.Unmarshal(merged, &doc) == nil {
			doc["_key"] = key
			h.index.Index(key, doc)
		}
	}
	h.mu.Unlock()

	h.writeSidecarFile(key, req.Data)
	respond(m, response{OK: true})

	// Publish merged state if entity exists.
	if h.msg != nil {
		h.mu.RLock()
		data, ok := h.data[key]
		h.mu.RUnlock()
		if ok {
			h.msg.Publish("state.changed."+key, data)
		}
	}
}

func (h *Handler) handleGet(m *messenger.Message) {
	var req getRequest
	if err := json.Unmarshal(m.Data, &req); err != nil {
		respond(m, response{Error: "bad request: " + err.Error()})
		return
	}

	h.mu.RLock()
	var (
		data json.RawMessage
		ok   bool
	)
	if req.Target == storage.Private {
		data, ok = h.private[req.Key]
	} else if req.Target == storage.Source {
		if src, exists := h.source[req.Key]; exists {
			encoded, err := json.Marshal(src)
			if err == nil {
				data = encoded
				ok = true
			}
		}
	} else if req.Target == storage.Profile {
		data, ok = h.sidecar[req.Key]
	} else if req.Target == storage.Internal {
		data, ok = h.internal[req.Key]
	} else if req.Target == storage.State {
		data, ok = h.state[req.Key]
	} else {
		data, ok = h.data[req.Key]
	}
	h.mu.RUnlock()

	if !ok {
		respond(m, response{Error: "not found: " + req.Key})
		return
	}
	respond(m, response{OK: true, Data: data})
}

func (h *Handler) handleDelete(m *messenger.Message) {
	var req deleteRequest
	if err := json.Unmarshal(m.Data, &req); err != nil {
		respond(m, response{Error: "bad request: " + err.Error()})
		return
	}

	h.mu.Lock()
	if req.Target == storage.Private {
		delete(h.private, req.Key)
		h.mu.Unlock()
		h.deletePrivateFile(req.Key)
		respond(m, response{OK: true})
		return
	}
	if req.Target == storage.Source {
		delete(h.source, req.Key)
		if existing, ok := h.state[req.Key]; ok {
			merged := mergeWithSidecar(existing, h.sidecar[req.Key])
			h.data[req.Key] = merged
			var doc map[string]any
			if json.Unmarshal(merged, &doc) == nil {
				doc["_key"] = req.Key
				h.index.Index(req.Key, doc)
			}
		}
		h.mu.Unlock()
		h.deleteLuaSourceFile(req.Key)
		respond(m, response{OK: true})
		return
	}
	if req.Target == storage.Profile {
		delete(h.sidecar, req.Key)
		if existing, ok := h.state[req.Key]; ok {
			h.data[req.Key] = mergeWithSource(existing, h.source[req.Key])
			var doc map[string]any
			if json.Unmarshal(h.data[req.Key], &doc) == nil {
				doc["_key"] = req.Key
				h.index.Index(req.Key, doc)
			}
		}
		h.mu.Unlock()
		h.deleteSidecarFile(req.Key)
		respond(m, response{OK: true})
		return
	}
	if req.Target == storage.Internal {
		delete(h.internal, req.Key)
		h.mu.Unlock()
		h.deleteInternalFile(req.Key)
		respond(m, response{OK: true})
		return
	}
	if req.Target == storage.State {
		delete(h.state, req.Key)
		delete(h.data, req.Key)
		delete(h.sidecar, req.Key)
		delete(h.private, req.Key)
		delete(h.internal, req.Key)
		delete(h.source, req.Key)
		h.index.Delete(req.Key)
		h.mu.Unlock()

		h.deleteFile(req.Key)
		h.deleteSidecarFile(req.Key)
		h.deletePrivateFile(req.Key)
		h.deleteInternalFile(req.Key)
		h.deleteLuaSourceFile(req.Key)
		respond(m, response{OK: true})
		return
	}
	delete(h.data, req.Key)
	delete(h.state, req.Key)
	delete(h.sidecar, req.Key)
	delete(h.private, req.Key)
	delete(h.internal, req.Key)
	delete(h.source, req.Key)
	h.index.Delete(req.Key)
	h.mu.Unlock()

	h.deleteFile(req.Key)
	h.deleteSidecarFile(req.Key)
	h.deletePrivateFile(req.Key)
	h.deleteInternalFile(req.Key)
	h.deleteLuaSourceFile(req.Key)
	respond(m, response{OK: true})
}

func (h *Handler) handleSearch(m *messenger.Message) {
	var req queryRequest
	if err := json.Unmarshal(m.Data, &req); err != nil {
		respond(m, response{Error: "bad request: " + err.Error()})
		return
	}

	h.mu.RLock()
	var entries []storage.Entry
	if req.Target == storage.Source {
		for key, src := range h.source {
			if !matchWildcard(req.Pattern, key) {
				continue
			}
			raw, err := json.Marshal(src)
			if err != nil {
				continue
			}
			entries = append(entries, storage.Entry{Key: key, Data: raw})
		}
		h.mu.RUnlock()
		respond(m, response{OK: true, Entries: entries})
		return
	}
	source := h.data
	if req.Target == storage.State {
		source = h.state
	} else if req.Target == storage.Profile {
		source = h.sidecar
	} else if req.Target == storage.Private {
		source = h.private
	} else if req.Target == storage.Internal {
		source = h.internal
	}
	for key, data := range source {
		if matchWildcard(req.Pattern, key) {
			entries = append(entries, storage.Entry{Key: key, Data: data})
		}
	}
	h.mu.RUnlock()

	respond(m, response{OK: true, Entries: entries})
}

func (h *Handler) handleQuery(m *messenger.Message) {
	var req queryRequest
	if err := json.Unmarshal(m.Data, &req); err != nil {
		respond(m, response{Error: "bad request: " + err.Error()})
		return
	}

	// If no filters, fall back to key pattern scan.
	if len(req.Where) == 0 {
		h.handleSearch(m)
		return
	}

	// Build Bleve query from filters.
	bq, err := buildBleveQuery(req.Where)
	if err != nil {
		respond(m, response{Error: "bad query: " + err.Error()})
		return
	}

	searchReq := bleve.NewSearchRequest(bq)
	searchReq.Size = 10000
	result, err := h.index.Search(searchReq)
	if err != nil {
		respond(m, response{Error: "search failed: " + err.Error()})
		return
	}

	h.mu.RLock()
	var entries []storage.Entry
	for _, hit := range result.Hits {
		key := hit.ID
		if req.Pattern != "" && !matchWildcard(req.Pattern, key) {
			continue
		}
		if data, ok := h.data[key]; ok {
			entries = append(entries, storage.Entry{Key: key, Data: data})
		}
	}
	h.mu.RUnlock()

	respond(m, response{OK: true, Entries: entries})
}

// Snapshot returns a copy of all stored data.
func (h *Handler) Snapshot() map[string]json.RawMessage {
	h.mu.RLock()
	defer h.mu.RUnlock()
	cp := make(map[string]json.RawMessage, len(h.data))
	for k, v := range h.data {
		cp[k] = v
	}
	return cp
}

// Load bulk-inserts data and re-indexes everything.
func (h *Handler) Load(data map[string]json.RawMessage) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for k, v := range data {
		h.state[k] = stripSourceField(stripUserFields(v))
		h.data[k] = v
		var doc map[string]any
		if json.Unmarshal(v, &doc) == nil {
			doc["_key"] = k
			h.index.Index(k, doc)
		}
	}
}

// Mock creates an in-memory storage service attached to the given messenger.
// Uses the full Bleve-backed handler. Returns a storage.Storage client.
func Mock(msg messenger.Messenger) (storage.Storage, error) {
	handler, err := NewHandler()
	if err != nil {
		return nil, err
	}
	if err := handler.Register(msg); err != nil {
		return nil, err
	}
	return storage.ClientFrom(msg), nil
}

// --- Sidecar helpers ---

// stripUserFields removes labels, meta, and profile from a JSON blob.
// Returns the original data unchanged if it has none of those fields.
func stripUserFields(data json.RawMessage) json.RawMessage {
	var m map[string]json.RawMessage
	if json.Unmarshal(data, &m) != nil {
		return data
	}
	changed := false
	for _, f := range userFields {
		if _, ok := m[f]; ok {
			delete(m, f)
			changed = true
		}
	}
	if !changed {
		return data
	}
	out, err := json.Marshal(m)
	if err != nil {
		return data
	}
	return out
}

// extractUserFields returns a JSON object containing only labels, meta,
// and profile from the input, or nil if none are present.
func extractUserFields(data json.RawMessage) json.RawMessage {
	var m map[string]json.RawMessage
	if json.Unmarshal(data, &m) != nil {
		return nil
	}
	extracted := make(map[string]json.RawMessage)
	for _, f := range userFields {
		if v, ok := m[f]; ok {
			extracted[f] = v
		}
	}
	if len(extracted) == 0 {
		return nil
	}
	out, err := json.Marshal(extracted)
	if err != nil {
		return nil
	}
	return out
}

// mergeWithSidecar overlays sidecar fields (labels, meta, profile) onto
// entity data. If sidecar is nil/empty, returns data unchanged.
func mergeWithSidecar(data, sidecar json.RawMessage) json.RawMessage {
	if len(sidecar) == 0 {
		return data
	}
	var base map[string]json.RawMessage
	if json.Unmarshal(data, &base) != nil {
		return data
	}
	var sc map[string]json.RawMessage
	if json.Unmarshal(sidecar, &sc) != nil {
		return data
	}
	for _, f := range userFields {
		if v, ok := sc[f]; ok {
			base[f] = v
		}
	}
	out, err := json.Marshal(base)
	if err != nil {
		return data
	}
	return out
}

func mergeWithSource(data json.RawMessage, source string) json.RawMessage {
	if source == "" {
		return data
	}
	var base map[string]json.RawMessage
	if json.Unmarshal(data, &base) != nil {
		return data
	}
	src, err := json.Marshal(source)
	if err != nil {
		return data
	}
	base["source"] = src
	out, err := json.Marshal(base)
	if err != nil {
		return data
	}
	return out
}

func stripSourceField(data json.RawMessage) json.RawMessage {
	var doc map[string]json.RawMessage
	if json.Unmarshal(data, &doc) != nil {
		return data
	}
	delete(doc, "source")
	out, err := json.Marshal(doc)
	if err != nil {
		return data
	}
	return out
}

func extractLuaSource(key string, data json.RawMessage) (string, bool) {
	if !isScriptDefinitionKey(key) {
		return "", false
	}
	var doc map[string]json.RawMessage
	if json.Unmarshal(data, &doc) != nil {
		return "", false
	}
	raw, ok := doc["source"]
	if !ok {
		return "", false
	}
	var source string
	if json.Unmarshal(raw, &source) != nil || source == "" {
		return "", false
	}
	return source, true
}

func isScriptDefinitionKey(key string) bool {
	return matchWildcard("sb-script.scripts.*", key)
}

func isJSONObject(data json.RawMessage) bool {
	var obj map[string]json.RawMessage
	return json.Unmarshal(data, &obj) == nil
}

// --- Wire types (must match storage-sdk wire format) ---

type saveRequest struct {
	Key    string                `json:"key"`
	Data   json.RawMessage       `json:"data"`
	Target storage.StorageTarget `json:"target,omitempty"`
}

type getRequest struct {
	Key    string                `json:"key"`
	Target storage.StorageTarget `json:"target,omitempty"`
}

type deleteRequest struct {
	Key    string                `json:"key"`
	Target storage.StorageTarget `json:"target,omitempty"`
}

type queryRequest struct {
	Pattern string                `json:"pattern"`
	Target  storage.StorageTarget `json:"target,omitempty"`
	Where   []storage.Filter      `json:"where,omitempty"`
}

type response struct {
	OK      bool            `json:"ok"`
	Data    json.RawMessage `json:"data,omitempty"`
	Entries []storage.Entry `json:"entries,omitempty"`
	Error   string          `json:"error,omitempty"`
}

func respond(m *messenger.Message, r response) {
	data, _ := json.Marshal(r)
	m.Respond(data)
}

// matchWildcard matches dot-delimited keys against patterns.
func matchWildcard(pattern, key string) bool {
	matched, _ := path.Match(
		strings.ReplaceAll(pattern, ">", "*"),
		key,
	)
	if matched {
		return true
	}
	if strings.Contains(pattern, ">") {
		prefix := strings.TrimSuffix(pattern, ">")
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	patParts := strings.Split(pattern, ".")
	keyParts := strings.Split(key, ".")
	if len(patParts) > 0 && patParts[len(patParts)-1] == ">" {
		prefix := patParts[:len(patParts)-1]
		if len(keyParts) < len(prefix) {
			return false
		}
		for i, p := range prefix {
			if p != "*" && p != keyParts[i] {
				return false
			}
		}
		return true
	}
	if len(patParts) != len(keyParts) {
		return false
	}
	for i, p := range patParts {
		if p == "*" {
			continue
		}
		if p != keyParts[i] {
			return false
		}
	}
	return true
}
