package server

import (
	"encoding/json"
	"testing"

	storage "github.com/slidebolt/sb-storage-sdk"
)

func TestProfileFilesCanBeReadSearchedAndDeletedByTarget(t *testing.T) {
	sch := setup(t)
	key := skey("plugin.device.entity")

	if err := sch.SetProfile(key, json.RawMessage(`{"labels":{"room":["kitchen"]}}`)); err != nil {
		t.Fatal(err)
	}

	rawProfile, err := sch.ReadFile(storage.Profile, key)
	if err != nil {
		t.Fatal(err)
	}
	if string(rawProfile) != `{"labels":{"room":["kitchen"]}}` {
		t.Fatalf("raw profile: got %s", rawProfile)
	}

	entries, err := sch.SearchFiles(storage.Profile, "plugin.>")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Key != key.Key() {
		t.Fatalf("profile search entries: %+v", entries)
	}

	if err := sch.DeleteFile(storage.Profile, key); err != nil {
		t.Fatal(err)
	}
	if _, err := sch.ReadFile(storage.Profile, key); err == nil {
		t.Fatal("expected deleted profile sidecar to be missing")
	}
}
