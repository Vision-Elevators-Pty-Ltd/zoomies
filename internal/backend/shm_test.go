package backend

import (
	"testing"
	"time"

	"github.com/eyupio/zoomies/internal/store"
)

// A runner container is created with the /dev/shm the controller resolved:
// Docker's 64 MB starves Chromium, which then captures empty screenshots.
func TestARunnerContainerCarriesItsShmSize(t *testing.T) {
	spec := jitSpec()
	spec.Resources.MemoryMB = 24 * 1024
	spec.ShmSizeMB = 2048
	cfg := buildRunnerConfig(spec, dockerFlavor(), containerOptions{Now: time.Now()})
	if got := cfg.HostConfig.ShmSize; got != 2048<<20 {
		t.Fatalf("ShmSize = %d, want 2 GiB (%d)", got, int64(2048<<20))
	}

	// Zero, which a controller older than the field also sends, leaves the
	// engine's own default: the field is omitted from the create request.
	spec.ShmSizeMB = 0
	if got := buildRunnerConfig(spec, dockerFlavor(), containerOptions{Now: time.Now()}).HostConfig.ShmSize; got != 0 {
		t.Fatalf("ShmSize = %d with none asked for, want 0 (the engine's 64 MB)", got)
	}
}

// The tmpfs's pages are charged to the runner's own memory limit, so a size
// past the limit could never be used and is held to it.
func TestTheShmSizeIsHeldToTheRunnersMemoryLimit(t *testing.T) {
	spec := jitSpec() // 4096 MB limit
	spec.ShmSizeMB = 8192
	if got := buildRunnerConfig(spec, dockerFlavor(), containerOptions{Now: time.Now()}).HostConfig.ShmSize; got != 4096<<20 {
		t.Fatalf("ShmSize = %d, want the 4 GiB limit", got)
	}
	spec.Resources = store.Resources{} // no limit: the size stands
	if got := buildRunnerConfig(spec, dockerFlavor(), containerOptions{Now: time.Now()}).HostConfig.ShmSize; got != 8192<<20 {
		t.Fatalf("ShmSize = %d without a limit, want 8 GiB", got)
	}
}

// In a docker-in-docker pair it is the runner container, where a browser test
// runs, that is given the size; the daemon sidecar keeps the engine's own.
func TestOnlyTheRunnerOfAPairIsGivenTheShmSize(t *testing.T) {
	spec := jitSpec()
	spec.DockerMode = store.DockerDinD
	spec.ShmSizeMB = 2048
	if got := buildRunnerConfig(spec, dockerFlavor(), containerOptions{Now: time.Now()}).HostConfig.ShmSize; got != 2048<<20 {
		t.Fatalf("the pair's runner has ShmSize %d, want 2 GiB", got)
	}
	if got := buildDinDConfig(spec, dockerFlavor(), containerOptions{Now: time.Now()}).HostConfig.ShmSize; got != 0 {
		t.Fatalf("the daemon sidecar has ShmSize %d, want the engine's own", got)
	}
}
