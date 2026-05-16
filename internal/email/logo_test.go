package email

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadLogoSuccess(t *testing.T) {
	p, err := loadLogo("testdata/logo_small.png")
	if err != nil {
		t.Fatalf("loadLogo: %v", err)
	}
	if p == nil {
		t.Fatal("loadLogo: nil part")
	}
	if p.CID != "jf-logo" {
		t.Errorf("CID: got %q want jf-logo", p.CID)
	}
	if p.Name != "logo_small.png" {
		t.Errorf("Name: got %q want logo_small.png", p.Name)
	}
	if len(p.Bytes) < 100 {
		t.Errorf("Bytes: got %d bytes, suspiciously small", len(p.Bytes))
	}
}

func TestLoadLogoMissing(t *testing.T) {
	_, err := loadLogo(filepath.Join(t.TempDir(), "nope.png"))
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoadLogoNotPNG(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "not_png.png")
	if err := os.WriteFile(path, []byte("this is not a png"), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	_, err := loadLogo(path)
	if err == nil {
		t.Fatal("expected error for non-PNG bytes")
	}
}

func TestLoadLogoTooBig(t *testing.T) {
	_, err := loadLogo("testdata/logo_too_big.png")
	if err == nil {
		t.Fatal("expected error for oversize PNG")
	}
}

func TestLoadLogoEmptyPathReturnsNilNil(t *testing.T) {
	p, err := loadLogo("")
	if err != nil {
		t.Fatalf("loadLogo(\"\"): unexpected error %v", err)
	}
	if p != nil {
		t.Fatalf("loadLogo(\"\"): expected nil, got %+v", p)
	}
}
