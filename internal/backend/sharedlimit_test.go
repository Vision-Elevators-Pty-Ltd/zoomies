package backend

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eyupio/zoomies/internal/store"
)

// fakeSlices records what a create and a removal asked of the slice manager.
type fakeSlices struct {
	mu       sync.Mutex
	limited  map[string]store.Resources
	released []string
	limitErr error
	// order is every call in sequence, so a test can see the slice was limited
	// before anything was created under it.
	order *[]string
}

func (f *fakeSlices) Limit(_ context.Context, unit string, res store.Resources) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.order != nil {
		*f.order = append(*f.order, "limit "+unit)
	}
	if f.limitErr != nil {
		return f.limitErr
	}
	if f.limited == nil {
		f.limited = map[string]store.Resources{}
	}
	f.limited[unit] = res
	return nil
}

func (f *fakeSlices) Release(_ context.Context, unit string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.released = append(f.released, unit)
	return nil
}

func sharedSpec() Spec {
	spec := jitSpec()
	spec.DockerMode = store.DockerDinD
	spec.DinDSharedLimit = true
	spec.Resources = store.Resources{CPUs: 4, MemoryMB: 8192, PidsLimit: 512}
	spec.ResourcesSource = store.AllocationFromPool
	spec.CPUTyped, spec.MemoryTyped = true, true
	return spec
}

// A dash in a slice name is a level of systemd's hierarchy, so a runner ID or a
// name with one in it must not scatter the pair across slices nobody limited.
func TestASharedSliceIsOneLevelBelowZoomiesWhateverTheRunnerIsCalled(t *testing.T) {
	for _, tc := range []struct {
		spec Spec
		want string
	}{
		{Spec{RunnerID: "run_ABC123"}, "zoomies-run_ABC123.slice"},
		{Spec{Name: "zoomies-linux-x64-7f3a"}, "zoomies-zoomies_linux_x64_7f3a.slice"},
		{Spec{RunnerID: "run/../x y"}, "zoomies-run____x_y.slice"},
		{Spec{}, "zoomies-runner.slice"},
		{Spec{RunnerID: strings.Repeat("a", 100)}, "zoomies-" + strings.Repeat("a", 64) + ".slice"},
	} {
		got := sharedSliceName(tc.spec)
		if got != tc.want {
			t.Errorf("sharedSliceName(%+v) = %q, want %q", tc.spec, got, tc.want)
		}
		if !sharedSliceUnit.MatchString(got) {
			t.Errorf("%q is not a name Release would act on", got)
		}
	}
}

// The slice holds the pair to what the scheduler charged: a typed field once,
// and a field the host sized as the slot the halves split.
func TestTheSharedSliceHoldsThePairToWhatItWasCharged(t *testing.T) {
	if got := sharedLimits(sharedSpec()); got.CPUs != 4 || got.MemoryMB != 8192 {
		t.Fatalf("typed: %+v; want 4 CPU and 8192 MB", got)
	}
	mixed := sharedSpec()
	mixed.ResourcesSource = store.AllocationFromHost
	mixed.CPUTyped = false
	mixed.Resources.CPUs = 3
	if got := sharedLimits(mixed); got.MemoryMB != 8192 || got.CPUs < 2.99 || got.CPUs > 3.01 {
		t.Fatalf("host-sized CPU: %+v; want the 3 CPU slot and the typed 8192 MB", got)
	}
}

// Both halves are created under the slice and say so, and each keeps a limit of
// its own no higher than the typed size: the slice bounds the two together, and
// neither container is ever unbounded on its own.
func TestBothHalvesOfASharedPairAreCreatedUnderItsSliceWithTheirOwnLimits(t *testing.T) {
	spec := sharedSpec()
	o := containerOptions{Now: time.Now(), DinDImage: DefaultDinDImage, Network: "zoomies", SharedSlice: "zoomies-run_123.slice"}
	runner := buildRunnerConfig(spec, dockerFlavor(), o)
	daemon := buildDinDConfig(spec, dockerFlavor(), o)
	for name, cfg := range map[string]ContainerCreateRequest{"runner": runner, "sidecar": daemon} {
		hc := cfg.HostConfig
		if hc.CgroupParent != "zoomies-run_123.slice" || cfg.Labels[LabelSharedSlice] != "zoomies-run_123.slice" {
			t.Errorf("%s: cgroup parent %q, label %q; want the pair's slice", name, hc.CgroupParent, cfg.Labels[LabelSharedSlice])
		}
		if hc.Memory <= 0 || hc.Memory > 8192<<20 || hc.NanoCPUs <= 0 || hc.NanoCPUs > 4e9 {
			t.Errorf("%s: memory %d, cpus %d; want a limit of its own no higher than the typed size", name, hc.Memory, hc.NanoCPUs)
		}
	}

	// Without the setting nothing changes: no parent, no label.
	plain := buildDinDConfig(spec, dockerFlavor(), containerOptions{Now: time.Now(), DinDImage: DefaultDinDImage})
	if plain.HostConfig.CgroupParent != "" || plain.Labels[LabelSharedSlice] != "" {
		t.Fatalf("a pair without a shared limit was given parent %q", plain.HostConfig.CgroupParent)
	}
}

// What is sent to systemd: runtime properties on the slice, memory in bytes and
// CPU as a whole percentage rounded up, through the manager the daemon's
// cgroups belong to.
func TestTheSliceIsLimitedThroughSystemctlSetProperty(t *testing.T) {
	var got [][]string
	var gotEnv []string
	run := func(_ context.Context, env []string, name string, args ...string) ([]byte, error) {
		got = append(got, append([]string{name}, args...))
		gotEnv = env
		return nil, nil
	}
	s := systemctlSlices{args: []string{"--user"}, env: []string{"XDG_RUNTIME_DIR=/run/user/1001"}, run: run}
	if err := s.Limit(context.Background(), "zoomies-run_1.slice", store.Resources{CPUs: 2.5, MemoryMB: 4096}); err != nil {
		t.Fatal(err)
	}
	want := []string{"systemctl", "--user", "set-property", "--runtime", "zoomies-run_1.slice", "MemoryMax=4294967296", "CPUQuota=250%"}
	if len(got) != 1 || !slices.Equal(got[0], want) {
		t.Fatalf("ran %v, want %v", got, want)
	}
	if !slices.Equal(gotEnv, []string{"XDG_RUNTIME_DIR=/run/user/1001"}) {
		t.Fatalf("env = %v", gotEnv)
	}

	got = nil
	if err := s.Release(context.Background(), "zoomies-run_1.slice"); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0][2] != "stop" || got[1][2] != "revert" {
		t.Fatalf("release ran %v; want stop then revert", got)
	}

	// A name read back from a label is not handed to systemctl unless it is one
	// of ours.
	got = nil
	if err := s.Release(context.Background(), "--now; reboot"); err == nil || len(got) != 0 {
		t.Fatalf("a foreign unit name was passed on: err=%v ran=%v", err, got)
	}
}

func TestTheSliceManagerFollowsTheDaemon(t *testing.T) {
	noenv := func(string) string { return "" }
	lookup := func(uid string) (string, error) {
		if uid == "1001" {
			return "ci", nil
		}
		return "", errors.New("unknown user")
	}
	rootless := SystemInfo{CgroupDriver: "systemd", SecurityOptions: []string{"name=rootless"}}
	for _, tc := range []struct {
		name     string
		info     SystemInfo
		socket   string
		euid     int
		wantArgs []string
		wantEnv  []string
		refused  string
	}{
		{name: "rootless as its own user", info: rootless, socket: "/run/user/1001/docker.sock", euid: 1001,
			wantArgs: []string{"--user"}, wantEnv: []string{"XDG_RUNTIME_DIR=/run/user/1001"}},
		{name: "rootless reached by root", info: rootless, socket: "/run/user/1001/docker.sock", euid: 0,
			wantArgs: []string{"--user", "--machine=ci@.host"}},
		{name: "rootless of another user", info: rootless, socket: "/run/user/1002/docker.sock", euid: 1001,
			refused: "uid 1002"},
		{name: "rootful as root", info: SystemInfo{CgroupDriver: "systemd"}, socket: "/var/run/docker.sock", euid: 0},
		{name: "rootful as a user", info: SystemInfo{CgroupDriver: "systemd"}, socket: "/var/run/docker.sock", euid: 1001,
			refused: "only root"},
		{name: "cgroupfs", info: SystemInfo{CgroupDriver: "cgroupfs", SecurityOptions: []string{"name=rootless"}}, socket: "/run/user/1001/docker.sock", euid: 1001,
			refused: "cgroupfs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := systemdSlicesFor(tc.info, tc.socket, tc.euid, noenv, lookup, execCommand)
			if tc.refused != "" {
				if !errors.Is(err, errSharedLimitUnsupported) || !strings.Contains(err.Error(), tc.refused) || !strings.Contains(err.Error(), "dind_shared_limit") {
					t.Fatalf("err = %v; want a refusal naming %q and the setting", err, tc.refused)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			s := got.(systemctlSlices)
			if !slices.Equal(s.args, tc.wantArgs) || !slices.Equal(s.env, tc.wantEnv) {
				t.Fatalf("args %v env %v; want %v %v", s.args, s.env, tc.wantArgs, tc.wantEnv)
			}
		})
	}
}

// sharedPairEngine is a daemon that creates and starts a pair, its sidecar
// healthy as soon as it is started, and records each create body by name.
func sharedPairEngine(t *testing.T, order *[]string) (*fakeEngine, map[string]ContainerCreateRequest, *sync.Mutex) {
	t.Helper()
	var mu sync.Mutex
	bodies := map[string]ContainerCreateRequest{}
	started := map[string]bool{}
	f := newFakeEngine(t, map[string]http.HandlerFunc{
		"GET " + v + "/images/{ref...}": func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, 200, map[string]string{"Id": "sha256:cached"})
		},
		"POST " + v + "/containers/create": func(w http.ResponseWriter, r *http.Request) {
			var body ContainerCreateRequest
			_ = json.NewDecoder(r.Body).Decode(&body)
			name := r.Form.Get("name")
			mu.Lock()
			bodies[name] = body
			*order = append(*order, "create "+name)
			mu.Unlock()
			writeJSON(w, 201, map[string]string{"Id": "id-" + name})
		},
		"GET " + v + "/containers/{id}/json": func(w http.ResponseWriter, r *http.Request) {
			id := r.PathValue("id")
			mu.Lock()
			up := started[id]
			mu.Unlock()
			if !strings.HasPrefix(id, "id-") {
				w.WriteHeader(404)
				return
			}
			state := &ContainerState{Status: "created"}
			if up {
				state = &ContainerState{Status: "running", Running: true, Health: &ContainerHealth{Status: "healthy"}}
			}
			writeJSON(w, 200, ContainerInspect{ID: id, State: state, Config: &ContainerConfig{}})
		},
		"POST " + v + "/containers/{id}/start": func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			started[r.PathValue("id")] = true
			mu.Unlock()
			w.WriteHeader(204)
		},
	})
	return f, bodies, &mu
}

// End to end against a daemon: the slice is limited before the sidecar exists,
// and both containers are created under it.
func TestASharedLimitCreateLimitsTheSliceBeforeEitherContainer(t *testing.T) {
	var order []string
	f, bodies, mu := sharedPairEngine(t, &order)
	b := dockerBackendFor(t, f, DockerOptions{})
	fs := &fakeSlices{order: &order}
	b.sharedSlices = fs

	spec := sharedSpec()
	if _, err := b.CreateWithResult(context.Background(), spec); err != nil {
		t.Fatalf("create: %v", err)
	}
	unit := sharedSliceName(spec)
	if got := fs.limited[unit]; got.CPUs != 4 || got.MemoryMB != 8192 {
		t.Fatalf("slice %s limited to %+v; want the typed size", unit, got)
	}
	mu.Lock()
	defer mu.Unlock()
	runner, sidecar := containerName(spec.Name), dindName(containerName(spec.Name))
	for _, name := range []string{runner, sidecar} {
		if bodies[name].HostConfig == nil || bodies[name].HostConfig.CgroupParent != unit {
			t.Errorf("%s was not created under %s", name, unit)
		}
	}
	if len(order) < 3 || order[0] != "limit "+unit || order[1] != "create "+sidecar {
		t.Fatalf("order = %v; want the slice limited before the sidecar is created", order)
	}
	if len(fs.released) != 0 {
		t.Fatalf("a successful create released its slice: %v", fs.released)
	}
}

// A pair charged once must not run with two whole limits, so a slice that
// cannot be limited refuses the create before any container exists.
func TestASharedLimitThatCannotBeAppliedRefusesTheCreate(t *testing.T) {
	var order []string
	f, bodies, mu := sharedPairEngine(t, &order)
	b := dockerBackendFor(t, f, DockerOptions{})
	b.sharedSlices = &fakeSlices{limitErr: errors.New("Failed to connect to bus"), order: &order}

	_, err := b.CreateWithResult(context.Background(), sharedSpec())
	if err == nil || !strings.Contains(err.Error(), "bus") {
		t.Fatalf("err = %v; want the slice's failure", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 0 {
		t.Fatalf("containers were created without their limit: %v", order)
	}
}

// The pool without the setting never touches systemd.
func TestADinDCreateWithoutTheSharedLimitLeavesSystemdAlone(t *testing.T) {
	var order []string
	f, bodies, mu := sharedPairEngine(t, &order)
	b := dockerBackendFor(t, f, DockerOptions{})
	fs := &fakeSlices{order: &order}
	b.sharedSlices = fs

	spec := sharedSpec()
	spec.DinDSharedLimit = false
	if _, err := b.CreateWithResult(context.Background(), spec); err != nil {
		t.Fatalf("create: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(fs.limited) != 0 {
		t.Fatalf("a pool without the shared limit limited a slice: %v", fs.limited)
	}
	for name, body := range bodies {
		if body.HostConfig.CgroupParent != "" {
			t.Errorf("%s was given a cgroup parent %q", name, body.HostConfig.CgroupParent)
		}
	}
}

// Removal lets the slice go once both containers are gone.
func TestRemovingASharedPairReleasesItsSlice(t *testing.T) {
	var mu sync.Mutex
	var deleted []string
	f := newFakeEngine(t, map[string]http.HandlerFunc{
		"GET " + v + "/containers/c1/json": func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, 200, ContainerInspect{ID: "c1", Config: &ContainerConfig{
				Labels: map[string]string{LabelName: "runner-1", LabelSharedSlice: "zoomies-run_1.slice"},
			}})
		},
		"DELETE " + v + "/containers/{id}": func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			deleted = append(deleted, r.PathValue("id"))
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		},
	})
	b := dockerBackendFor(t, f, DockerOptions{})
	fs := &fakeSlices{}
	b.sharedSlices = fs

	if err := b.Remove(context.Background(), "c1"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if !slices.Equal(fs.released, []string{"zoomies-run_1.slice"}) {
		t.Fatalf("released %v; want the pair's slice", fs.released)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(deleted) < 2 {
		t.Fatalf("deleted %v; want both containers before the slice", deleted)
	}
}
