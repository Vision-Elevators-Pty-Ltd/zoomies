package controller

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/eyupio/zoomies/internal/config"
	"github.com/eyupio/zoomies/internal/store"
)

func withRunnerSecretDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	old := RunnerSecretDir
	RunnerSecretDir = dir
	t.Cleanup(func() { RunnerSecretDir = old })
	return dir
}

// A pool holds a reference and the runner gets the file's value: the pool
// itself, which the API, the export and the database show, never holds it.
func TestSecretFileReferenceReachesTheRunnerNotThePool(t *testing.T) {
	dir := withRunnerSecretDir(t)
	if err := os.WriteFile(filepath.Join(dir, "REGISTRY_PULL_PASSWORD"), []byte("s3cret-value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	pool := &store.Pool{Env: map[string]string{
		"REGISTRY_PULL_PASSWORD": "secret-file:REGISTRY_PULL_PASSWORD",
		"REGISTRY_PULL_USERNAME": "pull-only",
	}}

	got, errs := resolveRunnerSecrets(runnerEnv(config.Runners{}, pool))
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if got["REGISTRY_PULL_PASSWORD"] != "s3cret-value" {
		t.Fatalf("the runner did not get the file's value (trailing newline trimmed): %q", got["REGISTRY_PULL_PASSWORD"])
	}
	if got["REGISTRY_PULL_USERNAME"] != "pull-only" {
		t.Fatalf("a plain value changed: %q", got["REGISTRY_PULL_USERNAME"])
	}
	if pool.Env["REGISTRY_PULL_PASSWORD"] != "secret-file:REGISTRY_PULL_PASSWORD" {
		t.Fatalf("resolving wrote the value back into the pool: %q", pool.Env["REGISTRY_PULL_PASSWORD"])
	}

	// A rotation is a new file: the next runner gets the new value with no
	// change to the pool.
	if err := os.WriteFile(filepath.Join(dir, "REGISTRY_PULL_PASSWORD"), []byte("rotated"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, _ = resolveRunnerSecrets(runnerEnv(config.Runners{}, pool))
	if got["REGISTRY_PULL_PASSWORD"] != "rotated" {
		t.Fatalf("a rotated file did not reach the next runner: %q", got["REGISTRY_PULL_PASSWORD"])
	}
}

// A reference that cannot be read leaves the variable out rather than
// handing the literal reference to a job as if it were the credential, and a
// name cannot reach outside the secret directory.
func TestUnreadableSecretFileIsDroppedNotPassedThrough(t *testing.T) {
	dir := withRunnerSecretDir(t)
	outside := filepath.Join(filepath.Dir(dir), "outside-"+filepath.Base(dir))
	if err := os.WriteFile(outside, []byte("host file"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(outside) })
	if err := os.WriteFile(filepath.Join(dir, "EMPTY"), []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "ADIR"), 0o700); err != nil {
		t.Fatal(err)
	}

	for _, ref := range []string{
		"secret-file:MISSING",
		"secret-file:../" + filepath.Base(outside),
		"secret-file:" + outside,
		"secret-file:",
		"secret-file:.hidden",
		"secret-file:EMPTY",
		"secret-file:ADIR",
	} {
		env, errs := resolveRunnerSecrets(map[string]string{"X": ref, "KEEP": "1"})
		if _, set := env["X"]; set {
			t.Errorf("%q: the variable was kept as %q", ref, env["X"])
		}
		if env["KEEP"] != "1" {
			t.Errorf("%q: an unrelated variable was lost", ref)
		}
		if len(errs) != 1 {
			t.Errorf("%q: errors = %v, want one", ref, errs)
		}
		for _, err := range errs {
			if strings.Contains(err.Error(), "host file") {
				t.Errorf("%q: an error carried file contents: %v", ref, err)
			}
		}
	}
}

func TestOversizedSecretFileIsRefused(t *testing.T) {
	dir := withRunnerSecretDir(t)
	if err := os.WriteFile(filepath.Join(dir, "BIG"), []byte(strings.Repeat("a", maxRunnerSecretBytes+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	env, errs := resolveRunnerSecrets(map[string]string{"X": "secret-file:BIG"})
	if _, set := env["X"]; set || len(errs) != 1 {
		t.Fatalf("an oversized file was used: set=%v errs=%v", set, errs)
	}
}
