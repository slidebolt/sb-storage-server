package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	messenger "github.com/slidebolt/sb-messenger-sdk"
	storage "github.com/slidebolt/sb-storage-sdk"
)

type blob struct {
	key  string
	data json.RawMessage
}

func (b blob) Key() string                  { return b.key }
func (b blob) MarshalJSON() ([]byte, error) { return b.data, nil }

func raw(key string, data string) blob {
	return blob{key: key, data: json.RawMessage(data)}
}

type skey string

func (s skey) Key() string { return string(s) }

func setup(t *testing.T) storage.Storage {
	t.Helper()
	msg, err := messenger.Mock()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { msg.Close() })

	sch, err := Mock(msg)
	if err != nil {
		t.Fatal(err)
	}
	return sch
}

func TestSaveAndGet(t *testing.T) {
	sch := setup(t)

	if err := sch.Save(raw("esphome.lightstrip.light001", `{"brightness":255,"color":"red"}`)); err != nil {
		t.Fatal(err)
	}

	got, err := sch.Get(skey("esphome.lightstrip.light001"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"brightness":255,"color":"red"}` {
		t.Errorf("got %s", got)
	}
}

func TestGetNotFound(t *testing.T) {
	sch := setup(t)

	_, err := sch.Get(skey("nonexistent.key"))
	if err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestDelete(t *testing.T) {
	sch := setup(t)

	sch.Save(raw("esphome.plug.switch01", `{"state":"on"}`))
	if err := sch.Delete(skey("esphome.plug.switch01")); err != nil {
		t.Fatal(err)
	}

	_, err := sch.Get(skey("esphome.plug.switch01"))
	if err == nil {
		t.Fatal("expected error after delete")
	}
}

func TestSearchWildcard(t *testing.T) {
	sch := setup(t)

	sch.Save(raw("esphome.lightstrip.light001", `{"b":100}`))
	sch.Save(raw("esphome.lightstrip.light002", `{"b":200}`))
	sch.Save(raw("esphome.plug.switch01", `{"s":"on"}`))
	sch.Save(raw("zigbee.bulb.light001", `{"b":50}`))

	tests := []struct {
		pattern string
		want    int
	}{
		{"esphome.lightstrip.*", 2},
		{"esphome.>", 3},
		{"*.*.light001", 2},
	}

	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			entries, err := sch.Search(tt.pattern)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != tt.want {
				keys := make([]string, len(entries))
				for i, e := range entries {
					keys[i] = e.Key
				}
				sort.Strings(keys)
				t.Errorf("pattern %q: got %d entries %v, want %d", tt.pattern, len(entries), keys, tt.want)
			}
		})
	}
}

func TestOverwrite(t *testing.T) {
	sch := setup(t)

	sch.Save(raw("esphome.lightstrip.light001", `{"b":100}`))
	sch.Save(raw("esphome.lightstrip.light001", `{"b":255}`))

	got, err := sch.Get(skey("esphome.lightstrip.light001"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"b":255}` {
		t.Errorf("got %s, want overwritten value", got)
	}
}

func TestQueryByField(t *testing.T) {
	sch := setup(t)

	sch.Save(raw("esphome.lr.light001", `{"type":"light","name":"Ceiling","state":{"power":true,"brightness":200}}`))
	sch.Save(raw("esphome.lr.switch01", `{"type":"switch","name":"Outlet","state":{"power":true}}`))
	sch.Save(raw("esphome.lr.sensor01", `{"type":"sensor","name":"Temperature","state":{"value":22.5}}`))

	// Query: all lights
	entries, err := sch.Query(storage.Query{
		Pattern: "esphome.>",
		Where:   []storage.Filter{{Field: "type", Op: storage.Eq, Value: "light"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("type=light: got %d entries, want 1", len(entries))
	}
	if entries[0].Key != "esphome.lr.light001" {
		t.Errorf("key: got %q, want esphome.lr.light001", entries[0].Key)
	}
}

func TestQueryNumericRange(t *testing.T) {
	sch := setup(t)

	sch.Save(raw("a.b.light1", `{"type":"light","state":{"brightness":50}}`))
	sch.Save(raw("a.b.light2", `{"type":"light","state":{"brightness":200}}`))
	sch.Save(raw("a.b.light3", `{"type":"light","state":{"brightness":100}}`))

	entries, err := sch.Query(storage.Query{
		Where: []storage.Filter{
			{Field: "state.brightness", Op: storage.Gt, Value: float64(100)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("brightness>100: got %d entries, want 1", len(entries))
	}
	if entries[0].Key != "a.b.light2" {
		t.Errorf("key: got %q, want a.b.light2", entries[0].Key)
	}
}

func TestQueryContains(t *testing.T) {
	sch := setup(t)

	sch.Save(raw("a.b.c1", `{"name":"Ceiling Light"}`))
	sch.Save(raw("a.b.c2", `{"name":"Floor Lamp"}`))
	sch.Save(raw("a.b.c3", `{"name":"Ceiling Fan"}`))

	entries, err := sch.Query(storage.Query{
		Where: []storage.Filter{
			{Field: "name", Op: storage.Contains, Value: "ceiling"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		keys := make([]string, len(entries))
		for i, e := range entries {
			keys[i] = e.Key
		}
		t.Fatalf("name contains ceiling: got %d entries %v, want 2", len(entries), keys)
	}
}

func TestQueryMultipleFilters(t *testing.T) {
	sch := setup(t)

	sch.Save(raw("a.b.e1", `{"type":"light","state":{"power":true,"brightness":200}}`))
	sch.Save(raw("a.b.e2", `{"type":"light","state":{"power":false,"brightness":0}}`))
	sch.Save(raw("a.b.e3", `{"type":"switch","state":{"power":true}}`))

	// Lights that are on
	entries, err := sch.Query(storage.Query{
		Where: []storage.Filter{
			{Field: "type", Op: storage.Eq, Value: "light"},
			{Field: "state.power", Op: storage.Eq, Value: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("type=light AND power=true: got %d entries, want 1", len(entries))
	}
	if entries[0].Key != "a.b.e1" {
		t.Errorf("key: got %q, want a.b.e1", entries[0].Key)
	}
}

func TestQueryNoFilters(t *testing.T) {
	sch := setup(t)

	sch.Save(raw("x.y.z1", `{"a":1}`))
	sch.Save(raw("x.y.z2", `{"a":2}`))

	// No filters, just pattern — behaves like Search
	entries, err := sch.Query(storage.Query{Pattern: "x.y.*"})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("no filters: got %d entries, want 2", len(entries))
	}
}

func TestQueryNeq(t *testing.T) {
	sch := setup(t)

	sch.Save(raw("a.b.e1", `{"type":"light"}`))
	sch.Save(raw("a.b.e2", `{"type":"switch"}`))
	sch.Save(raw("a.b.e3", `{"type":"sensor"}`))

	entries, err := sch.Query(storage.Query{
		Where: []storage.Filter{{Field: "type", Op: storage.Neq, Value: "light"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("type!=light: got %d entries, want 2", len(entries))
	}
	for _, e := range entries {
		if e.Key == "a.b.e1" {
			t.Error("should not include light")
		}
	}
}

func TestQueryGte(t *testing.T) {
	sch := setup(t)

	sch.Save(raw("a.b.s1", `{"value":10}`))
	sch.Save(raw("a.b.s2", `{"value":20}`))
	sch.Save(raw("a.b.s3", `{"value":30}`))

	entries, err := sch.Query(storage.Query{
		Where: []storage.Filter{{Field: "value", Op: storage.Gte, Value: float64(20)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("value>=20: got %d entries, want 2", len(entries))
	}
}

func TestQueryLt(t *testing.T) {
	sch := setup(t)

	sch.Save(raw("a.b.s1", `{"value":10}`))
	sch.Save(raw("a.b.s2", `{"value":20}`))
	sch.Save(raw("a.b.s3", `{"value":30}`))

	entries, err := sch.Query(storage.Query{
		Where: []storage.Filter{{Field: "value", Op: storage.Lt, Value: float64(20)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("value<20: got %d entries, want 1", len(entries))
	}
	if entries[0].Key != "a.b.s1" {
		t.Errorf("key: got %q, want a.b.s1", entries[0].Key)
	}
}

func TestQueryLte(t *testing.T) {
	sch := setup(t)

	sch.Save(raw("a.b.s1", `{"value":10}`))
	sch.Save(raw("a.b.s2", `{"value":20}`))
	sch.Save(raw("a.b.s3", `{"value":30}`))

	entries, err := sch.Query(storage.Query{
		Where: []storage.Filter{{Field: "value", Op: storage.Lte, Value: float64(20)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("value<=20: got %d entries, want 2", len(entries))
	}
}

func TestQueryPrefix(t *testing.T) {
	sch := setup(t)

	sch.Save(raw("a.b.c1", `{"name":"living-room-lamp"}`))
	sch.Save(raw("a.b.c2", `{"name":"living-room-fan"}`))
	sch.Save(raw("a.b.c3", `{"name":"bedroom-lamp"}`))

	entries, err := sch.Query(storage.Query{
		Where: []storage.Filter{{Field: "name", Op: storage.Prefix, Value: "living"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("name prefix living: got %d entries, want 2", len(entries))
	}
}

func TestQueryExists(t *testing.T) {
	sch := setup(t)

	sch.Save(raw("a.b.e1", `{"type":"light","effect":"rainbow"}`))
	sch.Save(raw("a.b.e2", `{"type":"light"}`))
	sch.Save(raw("a.b.e3", `{"type":"switch","effect":"pulse"}`))

	entries, err := sch.Query(storage.Query{
		Where: []storage.Filter{{Field: "effect", Op: storage.Exists}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("effect exists: got %d entries, want 2", len(entries))
	}
}

func TestQueryBoolField(t *testing.T) {
	sch := setup(t)

	sch.Save(raw("a.b.e1", `{"power":true}`))
	sch.Save(raw("a.b.e2", `{"power":false}`))
	sch.Save(raw("a.b.e3", `{"power":true}`))

	entries, err := sch.Query(storage.Query{
		Where: []storage.Filter{{Field: "power", Op: storage.Eq, Value: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("power=true: got %d entries, want 2", len(entries))
	}
}

func TestQueryPatternPlusFilter(t *testing.T) {
	sch := setup(t)

	sch.Save(raw("esphome.lr.light001", `{"type":"light","brightness":200}`))
	sch.Save(raw("esphome.lr.switch01", `{"type":"switch"}`))
	sch.Save(raw("zigbee.lr.light002", `{"type":"light","brightness":100}`))

	// Scoped to esphome + type=light
	entries, err := sch.Query(storage.Query{
		Pattern: "esphome.>",
		Where:   []storage.Filter{{Field: "type", Op: storage.Eq, Value: "light"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("esphome + type=light: got %d entries, want 1", len(entries))
	}
	if entries[0].Key != "esphome.lr.light001" {
		t.Errorf("key: got %q, want esphome.lr.light001", entries[0].Key)
	}
}

func TestQueryAfterDelete(t *testing.T) {
	sch := setup(t)

	sch.Save(raw("a.b.e1", `{"type":"light"}`))
	sch.Save(raw("a.b.e2", `{"type":"light"}`))
	sch.Delete(skey("a.b.e1"))

	entries, err := sch.Query(storage.Query{
		Where: []storage.Filter{{Field: "type", Op: storage.Eq, Value: "light"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("after delete: got %d entries, want 1", len(entries))
	}
	if entries[0].Key != "a.b.e2" {
		t.Errorf("key: got %q, want a.b.e2", entries[0].Key)
	}
}

func TestQueryAfterOverwrite(t *testing.T) {
	sch := setup(t)

	sch.Save(raw("a.b.e1", `{"type":"light","brightness":50}`))
	sch.Save(raw("a.b.e1", `{"type":"light","brightness":250}`))

	// Old value should not match
	entries, err := sch.Query(storage.Query{
		Where: []storage.Filter{{Field: "brightness", Op: storage.Gt, Value: float64(200)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("after overwrite: got %d entries, want 1", len(entries))
	}
}

func TestQueryNestedField(t *testing.T) {
	sch := setup(t)

	sch.Save(raw("a.b.e1", `{"state":{"power":true,"brightness":200}}`))
	sch.Save(raw("a.b.e2", `{"state":{"power":false,"brightness":0}}`))
	sch.Save(raw("a.b.e3", `{"state":{"power":true,"brightness":100}}`))

	// Nested: state.power=true AND state.brightness > 150
	entries, err := sch.Query(storage.Query{
		Where: []storage.Filter{
			{Field: "state.power", Op: storage.Eq, Value: true},
			{Field: "state.brightness", Op: storage.Gt, Value: float64(150)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("nested power=true + brightness>150: got %d entries, want 1", len(entries))
	}
	if entries[0].Key != "a.b.e1" {
		t.Errorf("key: got %q, want a.b.e1", entries[0].Key)
	}
}

func TestQueryIn(t *testing.T) {
	sch := setup(t)

	sch.Save(raw("a.b.e1", `{"type":"light"}`))
	sch.SetProfile(skey("a.b.e1"), json.RawMessage(`{"labels":{"group":["Mygroup"],"floor":["Basement"]}}`))
	sch.Save(raw("a.b.e2", `{"type":"light"}`))
	sch.SetProfile(skey("a.b.e2"), json.RawMessage(`{"labels":{"group":["Mygroup"],"floor":["Upstairs"]}}`))
	sch.Save(raw("a.b.e3", `{"type":"light"}`))
	sch.SetProfile(skey("a.b.e3"), json.RawMessage(`{"labels":{"group":["Other"],"floor":["Basement"]}}`))

	// group contains Mygroup AND floor contains Basement
	entries, err := sch.Query(storage.Query{
		Where: []storage.Filter{
			{Field: "labels.group", Op: storage.In, Value: "Mygroup"},
			{Field: "labels.floor", Op: storage.In, Value: "Basement"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		keys := make([]string, len(entries))
		for i, e := range entries {
			keys[i] = e.Key
		}
		t.Fatalf("group=Mygroup AND floor=Basement: got %d entries %v, want 1", len(entries), keys)
	}
	if entries[0].Key != "a.b.e1" {
		t.Errorf("key: got %q, want a.b.e1", entries[0].Key)
	}
}

func TestQueryInMultipleValues(t *testing.T) {
	sch := setup(t)

	sch.Save(raw("a.b.e1", `{"type":"light"}`))
	sch.SetProfile(skey("a.b.e1"), json.RawMessage(`{"labels":{"room":["kitchen","dining"]}}`))
	sch.Save(raw("a.b.e2", `{"type":"light"}`))
	sch.SetProfile(skey("a.b.e2"), json.RawMessage(`{"labels":{"room":["bedroom"]}}`))

	// "kitchen" is one of the values in the room label array
	entries, err := sch.Query(storage.Query{
		Where: []storage.Filter{
			{Field: "labels.room", Op: storage.In, Value: "kitchen"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("room in kitchen: got %d entries, want 1", len(entries))
	}
	if entries[0].Key != "a.b.e1" {
		t.Errorf("key: got %q, want a.b.e1", entries[0].Key)
	}
}

func TestQueryEmptyResult(t *testing.T) {
	sch := setup(t)

	sch.Save(raw("a.b.e1", `{"type":"light"}`))

	entries, err := sch.Query(storage.Query{
		Where: []storage.Filter{{Field: "type", Op: storage.Eq, Value: "nonexistent"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("empty result: got %d entries, want 0", len(entries))
	}
}

// --- Sidecar tests ---

func TestSaveStripsUserFields(t *testing.T) {
	sch := setup(t)

	// Save entity with labels in the data — labels should be stripped.
	sch.Save(raw("a.b.e1", `{"type":"light","name":"foo","labels":{"room":["kitchen"]}}`))

	data, err := sch.Get(skey("a.b.e1"))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	json.Unmarshal(data, &m)

	// Labels should NOT be present — they were stripped and no sidecar exists.
	if _, ok := m["labels"]; ok {
		t.Fatal("expected labels to be stripped from saved data")
	}
	// Core fields should be present.
	if m["type"] != "light" {
		t.Fatalf("type: got %v, want light", m["type"])
	}
}

func TestSetProfile(t *testing.T) {
	sch := setup(t)

	sch.Save(raw("a.b.e1", `{"type":"light","name":"foo"}`))
	sch.SetProfile(skey("a.b.e1"), json.RawMessage(`{"labels":{"room":["kitchen"]}}`))

	data, err := sch.Get(skey("a.b.e1"))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	json.Unmarshal(data, &m)

	labels, ok := m["labels"].(map[string]any)
	if !ok {
		t.Fatal("expected labels in merged result")
	}
	room, ok := labels["room"].([]any)
	if !ok || len(room) != 1 || room[0] != "kitchen" {
		t.Fatalf("labels.room: got %v, want [kitchen]", labels["room"])
	}
}

func TestSetProfileSurvivesRediscovery(t *testing.T) {
	sch := setup(t)

	// Initial save + profile.
	sch.Save(raw("a.b.e1", `{"type":"light","name":"foo"}`))
	sch.SetProfile(skey("a.b.e1"), json.RawMessage(`{"labels":{"room":["kitchen"]}}`))

	// Simulate plugin rediscovery: save again WITHOUT labels.
	sch.Save(raw("a.b.e1", `{"type":"light","name":"bar"}`))

	data, err := sch.Get(skey("a.b.e1"))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	json.Unmarshal(data, &m)

	// Name updated by plugin.
	if m["name"] != "bar" {
		t.Fatalf("name: got %v, want bar", m["name"])
	}
	// Labels survived the rediscovery save.
	labels, ok := m["labels"].(map[string]any)
	if !ok {
		t.Fatal("expected labels to survive rediscovery save")
	}
	room, ok := labels["room"].([]any)
	if !ok || len(room) != 1 || room[0] != "kitchen" {
		t.Fatalf("labels.room: got %v, want [kitchen]", labels["room"])
	}
}

func TestDeleteRemovesSidecar(t *testing.T) {
	sch := setup(t)

	sch.Save(raw("a.b.e1", `{"type":"light"}`))
	sch.SetProfile(skey("a.b.e1"), json.RawMessage(`{"labels":{"room":["kitchen"]}}`))
	sch.Delete(skey("a.b.e1"))

	_, err := sch.Get(skey("a.b.e1"))
	if err == nil {
		t.Fatal("expected entity to be deleted")
	}

	// Save again — should not have old sidecar labels.
	sch.Save(raw("a.b.e1", `{"type":"switch"}`))
	data, _ := sch.Get(skey("a.b.e1"))
	var m map[string]any
	json.Unmarshal(data, &m)
	if _, ok := m["labels"]; ok {
		t.Fatal("expected no labels after delete + re-save")
	}
}

func TestQueryMatchesSidecarLabels(t *testing.T) {
	sch := setup(t)

	sch.Save(raw("a.b.e1", `{"type":"light"}`))
	sch.SetProfile(skey("a.b.e1"), json.RawMessage(`{"labels":{"PluginAutomation":["Basement"]}}`))
	sch.Save(raw("a.b.e2", `{"type":"light"}`))

	entries, err := sch.Query(storage.Query{
		Where: []storage.Filter{
			{Field: "labels.PluginAutomation", Op: storage.Eq, Value: "Basement"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Key != "a.b.e1" {
		t.Fatalf("query by sidecar label: got %d entries, want 1 (a.b.e1)", len(entries))
	}
}

func TestSidecarWinsOverSaveLabels(t *testing.T) {
	sch := setup(t)

	sch.Save(raw("a.b.e1", `{"type":"light"}`))
	sch.SetProfile(skey("a.b.e1"), json.RawMessage(`{"labels":{"room":["kitchen"]}}`))

	// Plugin save includes different labels — sidecar should win.
	sch.Save(raw("a.b.e1", `{"type":"light","labels":{"room":["bedroom"]}}`))

	data, _ := sch.Get(skey("a.b.e1"))
	var m map[string]any
	json.Unmarshal(data, &m)
	labels := m["labels"].(map[string]any)
	room := labels["room"].([]any)
	if room[0] != "kitchen" {
		t.Fatalf("sidecar should win: got %v, want kitchen", room[0])
	}
}

func TestSetProfileBeforeEntity(t *testing.T) {
	sch := setup(t)

	// Set profile before entity exists.
	sch.SetProfile(skey("a.b.e1"), json.RawMessage(`{"labels":{"room":["kitchen"]}}`))

	// Entity doesn't exist yet.
	_, err := sch.Get(skey("a.b.e1"))
	if err == nil {
		t.Fatal("expected not found before entity save")
	}

	// Now save the entity — sidecar should be merged.
	sch.Save(raw("a.b.e1", `{"type":"light"}`))
	data, _ := sch.Get(skey("a.b.e1"))
	var m map[string]any
	json.Unmarshal(data, &m)
	labels, ok := m["labels"].(map[string]any)
	if !ok {
		t.Fatal("expected labels from pre-existing sidecar")
	}
	if labels["room"].([]any)[0] != "kitchen" {
		t.Fatal("expected kitchen from sidecar")
	}
}

func TestPrivateRoundTripAndNotVisibleInSearch(t *testing.T) {
	sch := setup(t)
	key := skey("a.b.e1")

	if err := sch.Save(raw(string(key), `{"type":"light","name":"foo"}`)); err != nil {
		t.Fatal(err)
	}
	if err := sch.SetPrivate(key, json.RawMessage(`{"apiKey":"secret","espKey":270689882}`)); err != nil {
		t.Fatal(err)
	}

	got, err := sch.GetPrivate(key)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"apiKey":"secret","espKey":270689882}` {
		t.Fatalf("private: got %s", got)
	}

	public, err := sch.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if string(public) != `{"type":"light","name":"foo"}` {
		t.Fatalf("public get leaked private data: %s", public)
	}

	entries, err := sch.Search("a.b.*")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Key != "a.b.e1" {
		t.Fatalf("search entries: %+v", entries)
	}
}

func TestDeleteRemovesPrivateSidecar(t *testing.T) {
	dir := t.TempDir()
	handler, err := NewHandlerWithDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := messenger.Mock()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { msg.Close() })
	if err := handler.Register(msg); err != nil {
		t.Fatal(err)
	}
	store := storage.ClientFrom(msg)

	key := skey("a.b.e1")
	if err := store.Save(raw(string(key), `{"type":"light"}`)); err != nil {
		t.Fatal(err)
	}
	if err := store.SetPrivate(key, json.RawMessage(`{"apiKey":"secret"}`)); err != nil {
		t.Fatal(err)
	}

	privatePath := filepath.Join(dir, "a", "b", "e1", "e1.private.json")
	if _, err := os.Stat(privatePath); err != nil {
		t.Fatalf("expected private sidecar on disk: %v", err)
	}

	if err := store.Delete(key); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(privatePath); !os.IsNotExist(err) {
		t.Fatalf("expected private sidecar deleted, stat err=%v", err)
	}
	if _, err := store.GetPrivate(key); err == nil {
		t.Fatal("expected private data to be deleted")
	}
	if _, err := os.Stat(filepath.Dir(privatePath)); !os.IsNotExist(err) {
		t.Fatalf("expected private sidecar directory deleted, stat err=%v", err)
	}
}

func TestPrivateMustBeJSONObject(t *testing.T) {
	sch := setup(t)

	err := sch.SetPrivate(skey("a.b.e1"), json.RawMessage(`["not","an","object"]`))
	if err == nil {
		t.Fatal("expected private write to reject non-object payload")
	}
}

func TestInternalRoundTripPersistsToSidecarAndReloads(t *testing.T) {
	dir := t.TempDir()
	handler, err := NewHandlerWithDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := messenger.Mock()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { msg.Close() })
	if err := handler.Register(msg); err != nil {
		t.Fatal(err)
	}
	store := storage.ClientFrom(msg)

	key := skey("plugin.device.entity")
	payload := json.RawMessage(`{"raw":{"source":"esphome"},"nativeKey":270689882}`)
	if err := store.SetInternal(key, payload); err != nil {
		t.Fatal(err)
	}

	internalPath := filepath.Join(dir, "plugin", "device", "entity", "entity.internal.json")
	if _, err := os.Stat(internalPath); err != nil {
		t.Fatalf("expected internal sidecar on disk: %v", err)
	}

	got, err := store.GetInternal(key)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("internal data: got %s want %s", got, payload)
	}

	entries, err := store.Search("plugin.device.*")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("internal sidecar should not appear in search: %+v", entries)
	}

	handler2, err := NewHandlerWithDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handler2.LoadFromDir(); err != nil {
		t.Fatal(err)
	}
	msg2, err := messenger.Mock()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { msg2.Close() })
	if err := handler2.Register(msg2); err != nil {
		t.Fatal(err)
	}
	store2 := storage.ClientFrom(msg2)

	got, err = store2.GetInternal(key)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("reloaded internal data: got %s want %s", got, payload)
	}
}

func TestDeleteRemovesInternalSidecarDirectory(t *testing.T) {
	dir := t.TempDir()
	handler, err := NewHandlerWithDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := messenger.Mock()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { msg.Close() })
	if err := handler.Register(msg); err != nil {
		t.Fatal(err)
	}
	store := storage.ClientFrom(msg)

	key := skey("sb-script.instances.999ff8cc52b4076f")
	payload := json.RawMessage(`{"name":"test","status":"running"}`)
	if err := store.SetInternal(key, payload); err != nil {
		t.Fatal(err)
	}

	internalPath := filepath.Join(dir, "sb-script", "instances", "999ff8cc52b4076f", "999ff8cc52b4076f.internal.json")
	if _, err := os.Stat(internalPath); err != nil {
		t.Fatalf("expected internal sidecar on disk: %v", err)
	}

	if err := store.DeleteInternal(key); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(internalPath); !os.IsNotExist(err) {
		t.Fatalf("expected internal sidecar deleted, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Dir(internalPath)); !os.IsNotExist(err) {
		t.Fatalf("expected internal sidecar directory deleted, stat err=%v", err)
	}
}

func TestScriptDefinitionPersistsLuaSidecarAndReloads(t *testing.T) {
	dir := t.TempDir()
	handler, err := NewHandlerWithDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := messenger.Mock()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { msg.Close() })
	if err := handler.Register(msg); err != nil {
		t.Fatal(err)
	}
	store := storage.ClientFrom(msg)

	key := skey("sb-script.scripts.party_time")
	v1 := raw(string(key), `{"type":"script","language":"lua","name":"party_time","source":"print(\"v1\")"}`)
	if err := store.Save(v1); err != nil {
		t.Fatal(err)
	}

	jsonPath := filepath.Join(dir, "sb-script", "scripts", "party_time", "party_time.json")
	luaPath := filepath.Join(dir, "sb-script", "scripts", "party_time", "party_time.lua")
	if _, err := os.Stat(jsonPath); err != nil {
		t.Fatalf("expected script json on disk: %v", err)
	}
	if _, err := os.Stat(luaPath); err != nil {
		t.Fatalf("expected script lua on disk: %v", err)
	}

	jsonBody, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(jsonBody), `"source"`) {
		t.Fatalf("script json should not embed source: %s", jsonBody)
	}

	luaBody, err := os.ReadFile(luaPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(luaBody) != `print("v1")` {
		t.Fatalf("lua body: got %q", luaBody)
	}

	got, err := store.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(got), `"source":"print(\"v1\")"`) {
		t.Fatalf("public get should merge source, got %s", got)
	}

	entries, err := store.Search("sb-script.scripts.*")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Key != string(key) {
		t.Fatalf("search entries: %+v", entries)
	}
	if !strings.Contains(string(entries[0].Data), `"source":"print(\"v1\")"`) {
		t.Fatalf("search entry should merge source, got %s", entries[0].Data)
	}

	rawState, err := store.ReadFile(storage.State, key)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(rawState), `"source"`) {
		t.Fatalf("raw state should not embed source, got %s", rawState)
	}

	rawSource, err := store.ReadFile(storage.Source, key)
	if err != nil {
		t.Fatal(err)
	}
	var source string
	if err := json.Unmarshal(rawSource, &source); err != nil {
		t.Fatal(err)
	}
	if source != `print("v1")` {
		t.Fatalf("raw source: got %q", source)
	}

	stateEntries, err := store.SearchFiles(storage.State, "sb-script.scripts.*")
	if err != nil {
		t.Fatal(err)
	}
	if len(stateEntries) != 1 || strings.Contains(string(stateEntries[0].Data), `"source"`) {
		t.Fatalf("raw state entries: %+v", stateEntries)
	}

	sourceEntries, err := store.SearchFiles(storage.Source, "sb-script.scripts.*")
	if err != nil {
		t.Fatal(err)
	}
	if len(sourceEntries) != 1 {
		t.Fatalf("raw source entries: %+v", sourceEntries)
	}
	if err := json.Unmarshal(sourceEntries[0].Data, &source); err != nil {
		t.Fatal(err)
	}
	if source != `print("v1")` {
		t.Fatalf("raw source search: got %q", source)
	}

	v2 := raw(string(key), `{"type":"script","language":"lua","name":"party_time","source":"print(\"v2\")"}`)
	if err := store.Save(v2); err != nil {
		t.Fatal(err)
	}
	luaBody, err = os.ReadFile(luaPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(luaBody) != `print("v2")` {
		t.Fatalf("updated lua body: got %q", luaBody)
	}

	handler2, err := NewHandlerWithDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handler2.LoadFromDir(); err != nil {
		t.Fatal(err)
	}
	msg2, err := messenger.Mock()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { msg2.Close() })
	if err := handler2.Register(msg2); err != nil {
		t.Fatal(err)
	}
	store2 := storage.ClientFrom(msg2)

	got, err = store2.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(got), `"source":"print(\"v2\")"`) {
		t.Fatalf("reloaded get should merge updated source, got %s", got)
	}
}

func TestProfileFieldsQueryableAfterUpdateAndReload(t *testing.T) {
	dir := t.TempDir()
	handler, err := NewHandlerWithDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := messenger.Mock()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { msg.Close() })
	if err := handler.Register(msg); err != nil {
		t.Fatal(err)
	}
	store := storage.ClientFrom(msg)

	key := skey("plugin.device.entity")
	if err := store.Save(raw(string(key), `{"type":"light","name":"fixture","state":{"power":true}}`)); err != nil {
		t.Fatal(err)
	}
	initialProfile := json.RawMessage(`{
		"labels":{"Room":["alpharoom"]},
		"meta":{"Display":{"priority":5}},
		"profile":{"name":"alphaname"}
	}`)
	if err := store.SetProfile(key, initialProfile); err != nil {
		t.Fatal(err)
	}

	assertQueryCount := func(st storage.Storage, field string, value any, want int) {
		t.Helper()
		entries, err := st.Query(storage.Query{
			Pattern: "plugin.device.*",
			Where:   []storage.Filter{{Field: field, Op: storage.Eq, Value: value}},
		})
		if err != nil {
			t.Fatalf("query %s=%v: %v", field, value, err)
		}
		if len(entries) != want {
			t.Fatalf("query %s=%v: got %d entries, want %d", field, value, len(entries), want)
		}
	}

	assertQueryCount(store, "labels.Room", "alpharoom", 1)
	assertQueryCount(store, "meta.Display.priority", float64(5), 1)
	assertQueryCount(store, "profile.name", "alphaname", 1)

	updatedProfile := json.RawMessage(`{
		"labels":{"Room":["betaroom"]},
		"meta":{"Display":{"priority":6}},
		"profile":{"name":"betaname"}
	}`)
	if err := store.SetProfile(key, updatedProfile); err != nil {
		t.Fatal(err)
	}

	assertQueryCount(store, "labels.Room", "alpharoom", 0)
	assertQueryCount(store, "labels.Room", "betaroom", 1)
	assertQueryCount(store, "meta.Display.priority", float64(5), 0)
	assertQueryCount(store, "meta.Display.priority", float64(6), 1)
	assertQueryCount(store, "profile.name", "alphaname", 0)
	assertQueryCount(store, "profile.name", "betaname", 1)

	handler2, err := NewHandlerWithDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handler2.LoadFromDir(); err != nil {
		t.Fatal(err)
	}
	msg2, err := messenger.Mock()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { msg2.Close() })
	if err := handler2.Register(msg2); err != nil {
		t.Fatal(err)
	}
	store2 := storage.ClientFrom(msg2)

	assertQueryCount(store2, "labels.Room", "betaroom", 1)
	assertQueryCount(store2, "meta.Display.priority", float64(6), 1)
	assertQueryCount(store2, "profile.name", "betaname", 1)
}

func TestQueryDefinitionsAreFirstClassStorageResources(t *testing.T) {
	dir := t.TempDir()
	handler, err := NewHandlerWithDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := messenger.Mock()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { msg.Close() })
	if err := handler.Register(msg); err != nil {
		t.Fatal(err)
	}
	store := storage.ClientFrom(msg)

	if err := storage.EnsureQueryLayout(store); err != nil {
		t.Fatal(err)
	}
	query := storage.Query{
		Pattern: "test.>",
		Where: []storage.Filter{
			{Field: "type", Op: storage.Eq, Value: "light"},
			{Field: "state.colorMode", Op: storage.Eq, Value: "rgb"},
		},
	}
	if err := storage.SaveQueryDefinition(store, "rgb_lights", query); err != nil {
		t.Fatal(err)
	}

	def, err := storage.GetQueryDefinition(store, "rgb_lights")
	if err != nil {
		t.Fatal(err)
	}
	if def.Type != "query" || def.Name != "rgb_lights" {
		t.Fatalf("unexpected definition: %+v", def)
	}
	if def.Query.Pattern != "test.>" || len(def.Query.Where) != 2 {
		t.Fatalf("unexpected query payload: %+v", def.Query)
	}

	defs, err := storage.ListQueryDefinitions(store)
	if err != nil {
		t.Fatal(err)
	}
	if len(defs) != 1 || defs[0].Name != "rgb_lights" {
		t.Fatalf("unexpected query definitions: %+v", defs)
	}

	entries, err := store.Query(storage.Query{
		Pattern: "sb-query.queries.*",
		Where:   []storage.Filter{{Field: "type", Op: storage.Eq, Value: "query"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Key != "sb-query.queries.rgb_lights" {
		t.Fatalf("query entries: %+v", entries)
	}
}

func TestMatchWildcard(t *testing.T) {
	tests := []struct {
		pattern string
		key     string
		want    bool
	}{
		{"esphome.lr.*", "esphome.lr.light001", true},
		{"esphome.lr.*", "esphome.lr.switch01", true},
		{"esphome.lr.*", "esphome.br.light001", false},
		{"esphome.>", "esphome.lr.light001", true},
		{"esphome.>", "esphome.lr.br.deep", true},
		{"esphome.>", "zigbee.lr.light001", false},
		{"*.lr.*", "esphome.lr.light001", true},
		{"*.lr.*", "zigbee.lr.switch01", true},
		{"esphome.lr.light001", "esphome.lr.light001", true},
		{"esphome.lr.light001", "esphome.lr.light002", false},
	}
	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.key, func(t *testing.T) {
			got := matchWildcard(tt.pattern, tt.key)
			if got != tt.want {
				t.Errorf("matchWildcard(%q, %q) = %v, want %v", tt.pattern, tt.key, got, tt.want)
			}
		})
	}
}
