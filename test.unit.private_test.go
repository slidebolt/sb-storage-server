package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	messenger "github.com/slidebolt/sb-messenger-sdk"
	storage "github.com/slidebolt/sb-storage-sdk"
)

// setupWithDir returns a storage client backed by a handler using tmpDir for
// persistence. Returns the client, the handler (so tests can Close/reopen),
// and the messenger (so tests can Close it on restart scenarios).
func setupWithDir(t *testing.T, dir string) (storage.Storage, *Handler, messenger.Messenger) {
	t.Helper()
	msg, err := messenger.Mock()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { msg.Close() })

	h, err := NewHandlerWithDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.LoadFromDir(); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if err := h.Register(msg); err != nil {
		t.Fatal(err)
	}
	return storage.ClientFrom(msg), h, msg
}

// payload with a top-level private blob plus public fields.
func devicePayloadWithPrivate() string {
	return `{"id":"cam1","plugin":"plugin-amcrest","name":"Cam","private":{"host":"192.168.88.121","username":"admin","password":"secret"}}`
}

func TestSave_PrivateFieldExtracted(t *testing.T) {
	sch := setup(t)

	if err := sch.Save(raw("plugin-amcrest.cam1", devicePayloadWithPrivate())); err != nil {
		t.Fatal(err)
	}

	// Main Get returns the stripped public doc.
	got, err := sch.Get(skey("plugin-amcrest.cam1"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(got), `"private"`) {
		t.Fatalf("private leaked into Get result: %s", got)
	}
	if !strings.Contains(string(got), `"name":"Cam"`) {
		t.Fatalf("public fields missing from Get result: %s", got)
	}

	// GetPrivate returns the raw private blob.
	priv, err := sch.GetPrivate(skey("plugin-amcrest.cam1"))
	if err != nil {
		t.Fatalf("GetPrivate: %v", err)
	}
	var creds map[string]string
	if err := json.Unmarshal(priv, &creds); err != nil {
		t.Fatalf("unmarshal private: %v", err)
	}
	if creds["host"] != "192.168.88.121" || creds["password"] != "secret" {
		t.Fatalf("private creds mismatch: %+v", creds)
	}
}

func TestSave_PrivateFieldNotInSearch(t *testing.T) {
	sch := setup(t)

	if err := sch.Save(raw("plugin-amcrest.cam1", devicePayloadWithPrivate())); err != nil {
		t.Fatal(err)
	}

	entries, err := sch.Search("plugin-amcrest.*")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(entries))
	}
	if strings.Contains(string(entries[0].Data), `"private"`) {
		t.Fatalf("private leaked into Search: %s", entries[0].Data)
	}
}

func TestSave_PrivateFieldStrippedAlongsideUserFields(t *testing.T) {
	sch := setup(t)

	// Save strips user-owned fields (meta/labels/profile) and the new private
	// field; SetProfile is the only way to persist user fields. Verify that
	// Save with all four present drops them all from the main doc and that
	// only private lands in the private sidecar.
	payload := `{"id":"cam1","plugin":"plugin-amcrest","name":"Cam","meta":{"room":"front"},"labels":{"tag":["cam"]},"private":{"password":"secret"}}`
	if err := sch.Save(raw("plugin-amcrest.cam1", payload)); err != nil {
		t.Fatal(err)
	}

	got, err := sch.Get(skey("plugin-amcrest.cam1"))
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"private", "meta", "labels"} {
		if strings.Contains(string(got), `"`+field+`"`) {
			t.Fatalf("field %q leaked into main doc: %s", field, got)
		}
	}

	priv, err := sch.GetPrivate(skey("plugin-amcrest.cam1"))
	if err != nil {
		t.Fatal(err)
	}
	var p map[string]string
	if err := json.Unmarshal(priv, &p); err != nil {
		t.Fatalf("unmarshal private: %v", err)
	}
	if p["password"] != "secret" {
		t.Fatalf("private payload wrong: %+v", p)
	}
}

func TestSave_PrivateReplaceSemantics(t *testing.T) {
	sch := setup(t)

	if err := sch.Save(raw("plugin-amcrest.cam1", `{"id":"cam1","private":{"password":"old"}}`)); err != nil {
		t.Fatal(err)
	}
	if err := sch.Save(raw("plugin-amcrest.cam1", `{"id":"cam1","private":{"password":"new"}}`)); err != nil {
		t.Fatal(err)
	}

	priv, err := sch.GetPrivate(skey("plugin-amcrest.cam1"))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]string
	json.Unmarshal(priv, &m)
	if m["password"] != "new" {
		t.Fatalf("expected replaced password=new, got %+v", m)
	}
}

func TestSave_PrivateOmittedLeavesExistingUntouched(t *testing.T) {
	sch := setup(t)

	if err := sch.Save(raw("plugin-amcrest.cam1", `{"id":"cam1","private":{"password":"keep"}}`)); err != nil {
		t.Fatal(err)
	}
	// Second save with no private key — must not clear the existing private blob.
	if err := sch.Save(raw("plugin-amcrest.cam1", `{"id":"cam1","name":"Cam"}`)); err != nil {
		t.Fatal(err)
	}

	priv, err := sch.GetPrivate(skey("plugin-amcrest.cam1"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(priv), `"password":"keep"`) {
		t.Fatalf("private was clobbered by unrelated Save: %s", priv)
	}
}

func TestSave_PrivateFieldNotInStateChangedEvent(t *testing.T) {
	msg, err := messenger.Mock()
	if err != nil {
		t.Fatal(err)
	}
	defer msg.Close()

	h, err := NewHandler()
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Register(msg); err != nil {
		t.Fatal(err)
	}
	sch := storage.ClientFrom(msg)

	received := make(chan []byte, 1)
	_, err = msg.Subscribe("state.changed.plugin-amcrest.cam1", func(m *messenger.Message) {
		select {
		case received <- append([]byte(nil), m.Data...):
		default:
		}
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := sch.Save(raw("plugin-amcrest.cam1", devicePayloadWithPrivate())); err != nil {
		t.Fatal(err)
	}

	select {
	case data := <-received:
		if strings.Contains(string(data), `"private"`) {
			t.Fatalf("private leaked into state.changed event: %s", data)
		}
		if !strings.Contains(string(data), `"name":"Cam"`) {
			t.Fatalf("event missing public fields: %s", data)
		}
	default:
		t.Fatal("no state.changed event received")
	}
}

func TestSave_PrivateSidecarPersistedToDisk(t *testing.T) {
	dir := t.TempDir()
	sch, _, _ := setupWithDir(t, dir)

	if err := sch.Save(raw("plugin-amcrest.cam1", devicePayloadWithPrivate())); err != nil {
		t.Fatal(err)
	}

	privatePath := filepath.Join(dir, "plugin-amcrest", "cam1", "cam1.private.json")
	data, err := os.ReadFile(privatePath)
	if err != nil {
		t.Fatalf("private sidecar missing on disk: %v", err)
	}
	var creds map[string]string
	if err := json.Unmarshal(data, &creds); err != nil {
		t.Fatalf("unmarshal sidecar: %v (raw=%s)", err, data)
	}
	if creds["password"] != "secret" || creds["host"] != "192.168.88.121" {
		t.Fatalf("sidecar contents wrong: %+v", creds)
	}

	// Main file must not contain private.
	mainPath := filepath.Join(dir, "plugin-amcrest", "cam1", "cam1.json")
	main, err := os.ReadFile(mainPath)
	if err != nil {
		t.Fatalf("main file missing: %v", err)
	}
	if strings.Contains(string(main), `"private"`) {
		t.Fatalf("private leaked into main file: %s", main)
	}
}

func TestLoadFromDir_PrivateSidecarSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	sch, h1, _ := setupWithDir(t, dir)

	if err := sch.Save(raw("plugin-amcrest.cam1", devicePayloadWithPrivate())); err != nil {
		t.Fatal(err)
	}
	h1.Close()

	// Fresh handler pointed at the same dir.
	sch2, _, _ := setupWithDir(t, dir)
	priv, err := sch2.GetPrivate(skey("plugin-amcrest.cam1"))
	if err != nil {
		t.Fatalf("GetPrivate after restart: %v", err)
	}
	if !strings.Contains(string(priv), `"password":"secret"`) {
		t.Fatalf("private did not survive restart: %s", priv)
	}
}
