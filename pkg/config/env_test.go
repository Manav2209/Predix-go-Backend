package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnvFilePreservesRuntimeOverrides(t *testing.T) {
	t.Setenv("PREDIX_ENV_OVERRIDE", "runtime")
	const loadedKey = "PREDIX_ENV_FILE_ONLY"
	old, exists := os.LookupEnv(loadedKey)
	os.Unsetenv(loadedKey)
	t.Cleanup(func() {
		if exists {
			os.Setenv(loadedKey, old)
		} else {
			os.Unsetenv(loadedKey)
		}
	})
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("# Local configuration\nPREDIX_ENV_OVERRIDE=file\nPREDIX_ENV_FILE_ONLY='configured'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := LoadEnvFile(path); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("PREDIX_ENV_OVERRIDE") != "runtime" || os.Getenv(loadedKey) != "configured" {
		t.Fatal("environment precedence or quoted parsing failed")
	}
}
