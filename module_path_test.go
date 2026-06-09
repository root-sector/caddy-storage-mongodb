package caddymongodb

import (
	"os"
	"strings"
	"testing"
)

func TestModulePathMatchesV2ReleaseLine(t *testing.T) {
	content, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}

	const want = "module github.com/root-sector/caddy-storage-mongodb/v2"
	if !strings.Contains(string(content), want) {
		t.Fatalf("go.mod must declare %q for v2.x tags", want)
	}
}
