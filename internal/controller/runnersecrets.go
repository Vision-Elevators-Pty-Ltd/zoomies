package controller

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/eyupio/zoomies/internal/store"
)

// A pool's env is readable to every operator in the API, the pool export
// keeps its keys, and the database keeps its values in the clear. A registry
// password typed into it is therefore a literal in all of those places.
//
// A value of the form "secret-file:NAME" keeps the credential out of the pool
// instead: the controller reads NAME from RunnerSecretDir on the controller's
// own host each time it builds a runner, and only the runner's environment
// ever holds the value. The pool (and so the API, the export, the audit row
// and the database) holds the reference. Whatever renders the file (a vault
// sync, a configuration manager) owns the value, and a rotation is a new file:
// runners created after it get the new value, without editing any pool.
const SecretFilePrefix = "secret-file:"

// RunnerSecretDir is where secret-file references are read from. It is a
// fixed directory, not a path the pool names, so a pool operator cannot point
// a runner's environment at an arbitrary file the controller can read. A
// variable so tests can point it elsewhere.
var RunnerSecretDir = "/etc/zoomies/runner-secrets"

// maxRunnerSecretBytes bounds one secret file; an environment variable is not
// the place for anything larger, and a mistaken symlink to a log must not be
// read whole into every runner.
const maxRunnerSecretBytes = 64 << 10

var runnerSecretName = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$`)

// resolveRunnerSecrets replaces every secret-file reference in env with the
// file's contents, minus trailing line endings. A reference that cannot be
// read is dropped from the environment rather than passed through: the
// literal "secret-file:..." is never a working credential, and a variable that
// is missing is something a job's own check can say, where a wrong value is a
// confusing authentication failure. Each failure is returned so the caller can
// log it; a runner is still created, because one unreadable secret must not
// stop a pool from scaling for every job that does not need it.
func resolveRunnerSecrets(env map[string]string) (map[string]string, []error) {
	var errs []error
	for k, v := range env {
		name, ok := strings.CutPrefix(v, SecretFilePrefix)
		if !ok {
			continue
		}
		value, err := readRunnerSecret(name)
		if err != nil {
			delete(env, k)
			errs = append(errs, fmt.Errorf("runner variable %s: %w", k, err))
			continue
		}
		env[k] = value
	}
	return env, errs
}

func readRunnerSecret(name string) (string, error) {
	if !runnerSecretName.MatchString(name) {
		return "", fmt.Errorf("secret-file name %q is not a plain file name", name)
	}
	f, err := os.Open(filepath.Join(RunnerSecretDir, name))
	if err != nil {
		return "", fmt.Errorf("secret-file %s: %w", name, err)
	}
	defer f.Close()
	if fi, err := f.Stat(); err != nil || !fi.Mode().IsRegular() {
		return "", fmt.Errorf("secret-file %s is not a regular file", name)
	}
	b, err := io.ReadAll(io.LimitReader(f, maxRunnerSecretBytes+1))
	if err != nil {
		return "", fmt.Errorf("secret-file %s: %w", name, err)
	}
	if len(b) > maxRunnerSecretBytes {
		return "", fmt.Errorf("secret-file %s is larger than %d bytes", name, maxRunnerSecretBytes)
	}
	value := strings.TrimRight(string(b), "\r\n")
	if value == "" {
		return "", errors.New("secret-file " + name + " is empty")
	}
	return value, nil
}

// poolRunnerEnv is runnerEnv with the pool's secret-file references read,
// logging any that could not be. The values are never logged.
func (c *Controller) poolRunnerEnv(pool *store.Pool) map[string]string {
	env, errs := resolveRunnerSecrets(runnerEnv(c.cfg().Runners, pool))
	for _, err := range errs {
		c.log.Error("a runner starts without a secret its pool names", "pool", pool.Name, "error", err)
	}
	return env
}
