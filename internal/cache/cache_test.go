package cache

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"flipgroup.com.au/jellyfish/internal/iru"
)

func TestSaveAndLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "detections.json")
	dets := []iru.Detection{
		{CVEID: "CVE-1", DeviceID: "d-1"},
		{CVEID: "CVE-2", DeviceID: "d-2"},
	}
	if err := Save(path, dets); err != nil {
		t.Fatalf("save: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("expected 0600, got %o", info.Mode().Perm())
	}
	out, hit, err := Load(path, 1*time.Minute)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !hit {
		t.Fatal("expected hit")
	}
	if len(out) != 2 || out[0].CVEID != "CVE-1" {
		t.Fatalf("bad payload: %+v", out)
	}
}

func TestLoadMissingFile(t *testing.T) {
	out, hit, err := Load(filepath.Join(t.TempDir(), "missing.json"), 1*time.Minute)
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if hit {
		t.Fatal("expected miss")
	}
	if out != nil {
		t.Fatalf("expected nil, got %+v", out)
	}
}

func TestLoadExpired(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "detections.json")
	if err := Save(path, []iru.Detection{{CVEID: "CVE-old"}}); err != nil {
		t.Fatal(err)
	}
	// Re-write with an old timestamp by editing the file directly.
	data, _ := os.ReadFile(path)
	// Use a small TTL so the current entry is expired immediately by sleeping briefly.
	time.Sleep(20 * time.Millisecond)
	_, hit, err := Load(path, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if hit {
		t.Fatal("expected miss because cache is expired")
	}
	_ = data
}

func TestLoadCorrupt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "detections.json")
	if err := os.WriteFile(path, []byte("{not valid json"), 0o600); err != nil {
		t.Fatal(err)
	}
	out, hit, err := Load(path, 1*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if hit || out != nil {
		t.Fatal("expected silent miss on corrupt cache")
	}
}
