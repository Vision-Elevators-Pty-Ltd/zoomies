package agent

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/eyupio/zoomies/internal/backend"
	"github.com/eyupio/zoomies/internal/store"
)

type workerProbeBackend struct {
	*fakeBackend
	present    bool
	probeError error
}

func (b *workerProbeBackend) RunnerWorkerPresent(context.Context, backend.Handle) (bool, error) {
	return b.present, b.probeError
}

func TestAnIdleStopRefusesAWorkerOrAnUncertainProbeWithoutMarkingItStopped(t *testing.T) {
	for _, tc := range []struct {
		name        string
		present     bool
		probeError  error
		unsupported bool
		wantStop    bool
	}{
		{"worker present", true, nil, false, false},
		{"worker absent", false, nil, false, true},
		{"probe error", false, errors.New("probe unavailable"), false, false},
		{"unsupported backend", false, nil, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			be := newFakeBackend(store.BackendDocker)
			var implementation backend.Backend = &workerProbeBackend{fakeBackend: be, present: tc.present, probeError: tc.probeError}
			if tc.unsupported {
				implementation = be
			}
			h := newHarness(t, 1, func(o *Options) { o.Backends = backend.NewRegistry(implementation) })
			h.tr.tasks <- []Task{createTask("create-1", "runner-1")}
			if res := h.nextResult(); !res.OK {
				t.Fatalf("create failed: %+v", res)
			}
			if !slices.Contains(h.agent.features(), FeatureIdleWorkerGuard) {
				t.Fatal("agent does not advertise the guarded task")
			}
			h.tr.tasks <- []Task{{ID: "stop-1", Kind: TaskStopIdleRunner, RunnerID: "runner-1"}}
			res := h.nextResult()
			if res.OK != tc.wantStop || res.Kind != TaskStopIdleRunner || res.State != "" {
				t.Fatalf("stop result = %+v, want OK %v and no exit claim", res, tc.wantStop)
			}
			_, stopped, removed := be.counts()
			want := 0
			if tc.wantStop {
				want = 1
			}
			if stopped != want || removed != 0 {
				t.Fatalf("stop/remove calls = %d/%d, want %d/0", stopped, removed, want)
			}
			h.agent.mu.Lock()
			stopping := h.agent.runners["runner-1"].stopping
			h.agent.mu.Unlock()
			if stopping != tc.wantStop {
				t.Fatalf("stopping = %v, want %v", stopping, tc.wantStop)
			}
		})
	}
}
