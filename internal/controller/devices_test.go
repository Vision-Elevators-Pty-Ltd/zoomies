package controller

import (
	"strings"
	"testing"
	"time"

	"github.com/eyupio/zoomies/internal/agent"
	"github.com/eyupio/zoomies/internal/store"
)

// A pool that maps /dev/kvm selects kvm=ok hosts, and the ordinary host
// selector places it only there; the create task carries the device.
func TestAKVMPoolIsPlacedOnlyOnKVMHostsAndItsRunnersGetTheDevice(t *testing.T) {
	h := newHarness(t)
	inst := h.installation()
	pool := h.pool(inst, "linux-x64")
	pool.MinRunners = 1
	pool.HostSelector = store.StringMap{"kvm": "ok"}
	pool.RunnerSettings.Devices = []string{"/dev/kvm"}
	pool.RunnerGroup = h.emulatorGroup()
	if err := h.st.UpdatePool(h.ctx, pool); err != nil {
		t.Fatalf("UpdatePool: %v", err)
	}
	plain := h.measuredHost("sa-ci-01", 8, 32768, 4, enforcesEverything)
	kvm := h.kvmHost("ci-nl-02")
	if err := h.c.Reconcile(h.ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	h.c.lifecycleCalls.Wait()
	for _, task := range h.tasksFor(plain.ID) {
		if task.Kind == agent.TaskCreateRunner {
			t.Fatal("the kvm pool was placed on a host without kvm=ok")
		}
	}
	task := h.taskOfKind(kvm.ID, agent.TaskCreateRunner)
	if task.Spec == nil || len(task.Spec.Devices) != 1 || task.Spec.Devices[0] != "/dev/kvm" {
		t.Fatalf("the create task's spec carries devices %v", task.Spec)
	}
}

// Every other pool's runners are created as before: no devices.
func TestAPoolWithoutDevicesSendsNone(t *testing.T) {
	h := newHarness(t)
	inst := h.installation()
	pool := h.pool(inst, "linux-x64")
	pool.MinRunners = 1
	if err := h.st.UpdatePool(h.ctx, pool); err != nil {
		t.Fatalf("UpdatePool: %v", err)
	}
	host := h.measuredHost("ci-nl-01", 8, 32768, 4, enforcesEverything)
	if err := h.c.Reconcile(h.ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	h.c.lifecycleCalls.Wait()
	if task := h.taskOfKind(host.ID, agent.TaskCreateRunner); task.Spec == nil || task.Spec.Devices != nil {
		t.Fatalf("spec devices = %v, want none", task.Spec)
	}
}

// emulatorGroup is a runner group GitHub restricts to selected workflows and
// keeps from public repositories: what a pool that maps devices registers in.
func (h *harness) emulatorGroup() string {
	h.t.Helper()
	h.gh.AddRunnerGroup("emulator")
	h.gh.SetRunnerGroupPublicAccess("emulator", false)
	h.gh.SetRunnerGroupRestrictedToWorkflows("emulator", true)
	return "emulator"
}

// kvmHost is a host labelled kvm=ok whose agent maps devices and is reserved
// for device pools (agent.devices_only).
func (h *harness) kvmHost(name string) *store.Host {
	h.t.Helper()
	host := h.measuredHost(name, 8, 32768, 4, enforcesEverything)
	labels := store.StringMap{"kvm": "ok"}
	if err := h.st.PatchHost(h.ctx, host.ID, store.HostChanges{Labels: &labels}); err != nil {
		h.t.Fatalf("PatchHost: %v", err)
	}
	host.Features = append(host.Features, agent.FeatureDevices, agent.FeatureDevicesOnly)
	if err := h.st.SetHostReported(h.ctx, host); err != nil {
		h.t.Fatalf("SetHostReported: %v", err)
	}
	return host
}

// kvmCreateQueued is a kvm pool with one runner placed on host, its create
// task waiting in the host's queue.
func (h *harness) kvmCreateQueued(host *store.Host) (*store.Pool, string) {
	h.t.Helper()
	inst := h.installation()
	pool := h.pool(inst, "emulator")
	pool.MinRunners = 1
	pool.HostSelector = store.StringMap{"kvm": "ok"}
	pool.RunnerSettings.Devices = []string{"/dev/kvm"}
	pool.RunnerGroup = h.emulatorGroup()
	if err := h.st.UpdatePool(h.ctx, pool); err != nil {
		h.t.Fatalf("UpdatePool: %v", err)
	}
	if err := h.c.Reconcile(h.ctx); err != nil {
		h.t.Fatalf("Reconcile: %v", err)
	}
	h.c.lifecycleCalls.Wait()
	task := h.taskOfKind(host.ID, agent.TaskCreateRunner)
	return pool, task.RunnerID
}

// pollCreates is what one poll by the host hands it of create tasks.
func (h *harness) pollCreates(host *store.Host) []agent.Task {
	h.t.Helper()
	batch, err := h.c.PollTasks(h.ctx, host.ID, 10*time.Millisecond)
	if err != nil {
		h.t.Fatalf("PollTasks: %v", err)
	}
	var out []agent.Task
	for _, t := range batch.Tasks {
		if t.Kind == agent.TaskCreateRunner {
			out = append(out, t)
		}
	}
	return out
}

// Review of #23, findings 1 and 4: a create queued before its pool's devices
// were cleared, or before its host lost kvm=ok (or its device-capable agent),
// is not delivered when the host polls for it. The runner is failed with the
// reason and its lease cleared, so it is never redelivered.
func TestADeviceCreateQueuedBeforeAChangeIsNotDelivered(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(h *harness, pool *store.Pool, host *store.Host)
		why    string
	}{
		{"the pool's devices cleared", func(h *harness, pool *store.Pool, _ *store.Host) {
			pool.RunnerSettings.Devices = nil
			pool.HostSelector = store.StringMap{}
			if err := h.st.UpdatePool(h.ctx, pool); err != nil {
				h.t.Fatal(err)
			}
		}, "devices changed"},
		{"the host's kvm=ok label removed", func(h *harness, _ *store.Pool, host *store.Host) {
			none := store.StringMap{}
			if err := h.st.PatchHost(h.ctx, host.ID, store.HostChanges{Labels: &none}); err != nil {
				h.t.Fatal(err)
			}
		}, "no longer takes"},
		{"the host's agent no longer maps devices", func(h *harness, _ *store.Pool, host *store.Host) {
			host.Features = store.StringSlice{agent.FeatureDaemonShare, agent.FeatureDevicesOnly}
			if err := h.st.SetHostReported(h.ctx, host); err != nil {
				h.t.Fatal(err)
			}
		}, "no longer takes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			host := h.kvmHost("ci-nl-02")
			pool, runnerID := h.kvmCreateQueued(host)
			tc.change(h, pool, host)
			if creates := h.pollCreates(host); len(creates) != 0 {
				t.Fatalf("the host was handed %d create(s) after the change", len(creates))
			}
			got := h.runnerByID(t, runnerID)
			if got.State != store.RunnerFailed || !strings.Contains(got.Message, tc.why) {
				t.Fatalf("runner %s: %q, want failed saying %q", got.State, got.Message, tc.why)
			}
			q := h.c.queues.get(host.ID)
			q.mu.Lock()
			for _, lt := range q.inflight {
				if lt.task.RunnerID == runnerID {
					t.Error("the withheld create is still leased for redelivery")
				}
			}
			q.mu.Unlock()
		})
	}

	// Nothing changed: the create is delivered with its device.
	h := newHarness(t)
	host := h.kvmHost("ci-nl-02")
	h.kvmCreateQueued(host)
	if creates := h.pollCreates(host); len(creates) != 1 || len(creates[0].Spec.Devices) != 1 {
		t.Fatalf("an unchanged kvm create was not delivered: %+v", creates)
	}
}

// Review of #23, finding 1, end to end: clearing a kvm pool's devices through
// the store's write stamps the change, and the next pass drains the idle
// runner that still has /dev/kvm mapped.
func TestClearingAPoolsDevicesDrainsItsIdleRunners(t *testing.T) {
	h := newHarness(t)
	inst := h.installation()
	pool := h.pool(inst, "emulator")
	pool.IdleTimeout = 0
	pool.HostSelector = store.StringMap{"kvm": "ok"}
	pool.RunnerSettings.Devices = []string{"/dev/kvm"}
	if err := h.st.UpdatePool(h.ctx, pool); err != nil {
		t.Fatal(err)
	}
	host := h.kvmHost("ci-nl-02")
	idle := h.runnerRow(pool, host, store.RunnerIdle)
	time.Sleep(5 * time.Millisecond) // rows are stamped to the millisecond
	pool.RunnerSettings.Devices = nil
	pool.HostSelector = store.StringMap{}
	pool.RunnerSettings.DevicesChangedAt = &time.Time{} // what the API sets
	if err := h.st.UpdatePool(h.ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err := h.c.Reconcile(h.ctx); err != nil {
		t.Fatal(err)
	}
	h.c.lifecycleCalls.Wait()
	if got := h.runnerByID(t, idle.ID); got.State != store.RunnerDraining {
		t.Fatalf("the idle runner made with /dev/kvm is %s, want draining", got.State)
	}
}

// The approved design: the runner-emu principal's agent is reserved for
// device pools, so a general pool whose selector matches it is never placed
// there, and an agent that predates devices is never given a device pool.
func TestAReservedAgentTakesOnlyDevicePools(t *testing.T) {
	h := newHarness(t)
	inst := h.installation()
	general := h.pool(inst, "pr")
	general.MinRunners = 2
	if err := h.st.UpdatePool(h.ctx, general); err != nil {
		t.Fatal(err)
	}
	emu := h.kvmHost("ci-nl-01-emu")
	plain := h.measuredHost("ci-nl-01", 8, 32768, 4, enforcesEverything)
	if err := h.c.Reconcile(h.ctx); err != nil {
		t.Fatal(err)
	}
	h.c.lifecycleCalls.Wait()
	if h.hasTaskOfKind(emu.ID, agent.TaskCreateRunner) {
		t.Fatal("a general runner was placed on the reserved emulator agent")
	}
	if !h.hasTaskOfKind(plain.ID, agent.TaskCreateRunner) {
		t.Fatal("the general pool was not placed on the general host")
	}
}

// Review r2 of #23, finding 4: a create queued without devices for a pool that
// has since gained them, or for a host since reserved for device pools, is not
// delivered; an ordinary create that nothing changed still is.
func TestAnOrdinaryCreateQueuedBeforeAChangeIsNotDelivered(t *testing.T) {
	queue := func(h *harness) (*store.Pool, *store.Host, string) {
		inst := h.installation()
		pool := h.pool(inst, "pr")
		pool.MinRunners = 1
		if err := h.st.UpdatePool(h.ctx, pool); err != nil {
			h.t.Fatal(err)
		}
		host := h.measuredHost("ci-nl-01", 8, 32768, 4, enforcesEverything)
		if err := h.c.Reconcile(h.ctx); err != nil {
			h.t.Fatal(err)
		}
		h.c.lifecycleCalls.Wait()
		task := h.taskOfKind(host.ID, agent.TaskCreateRunner)
		if len(task.Spec.Devices) != 0 {
			h.t.Fatalf("the ordinary create maps %v", task.Spec.Devices)
		}
		return pool, host, task.RunnerID
	}
	for _, tc := range []struct {
		name   string
		change func(h *harness, pool *store.Pool, host *store.Host)
		why    string
	}{
		{"the pool gained /dev/kvm", func(h *harness, pool *store.Pool, _ *store.Host) {
			pool.RunnerSettings.Devices = []string{"/dev/kvm"}
			pool.HostSelector = store.StringMap{"kvm": "ok"}
			pool.RunnerSettings.DevicesChangedAt = &time.Time{}
			if err := h.st.UpdatePool(h.ctx, pool); err != nil {
				h.t.Fatal(err)
			}
		}, "now maps /dev/kvm"},
		{"the host was reserved for device pools", func(h *harness, _ *store.Pool, host *store.Host) {
			host.Features = append(host.Features, agent.FeatureDevices, agent.FeatureDevicesOnly)
			if err := h.st.SetHostReported(h.ctx, host); err != nil {
				h.t.Fatal(err)
			}
		}, "reserved for pools that map devices"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			pool, host, runnerID := queue(h)
			tc.change(h, pool, host)
			if creates := h.pollCreates(host); len(creates) != 0 {
				t.Fatalf("the host was handed %d create(s) after the change", len(creates))
			}
			if got := h.runnerByID(t, runnerID); got.State != store.RunnerFailed || !strings.Contains(got.Message, tc.why) {
				t.Fatalf("runner %s: %q, want failed saying %q", got.State, got.Message, tc.why)
			}
		})
	}
	h := newHarness(t)
	_, host, _ := queue(h)
	if creates := h.pollCreates(host); len(creates) != 1 {
		t.Fatalf("an unchanged ordinary create was not delivered: %+v", creates)
	}
}

// Review r2 of #23, finding 2: an agent reserved for device pools and rolled
// back to a release that does not say so (or restarted with the setting only
// in its environment, which an older release ignores) keeps its host reserved:
// its heartbeat does not lift the reservation, and no general pool is placed
// there, nor a device pool, its agent no longer mapping devices.
func TestAReservedHostStaysReservedWhenItsAgentStopsSayingSo(t *testing.T) {
	h := newHarness(t)
	inst := h.installation()
	general := h.pool(inst, "pr")
	general.MinRunners = 1
	if err := h.st.UpdatePool(h.ctx, general); err != nil {
		t.Fatal(err)
	}
	emu := h.kvmHost("ci-nl-01-emu")
	// The rolled-back agent's heartbeat: its old features, no devices ones.
	if _, err := h.c.Heartbeat(h.ctx, emu.ID, agent.HeartbeatRequest{Features: []string{agent.FeatureElasticCPU, agent.FeatureDaemonShare}}); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	got, err := h.st.GetHost(h.ctx, emu.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Supports(agent.FeatureDevicesOnly) || got.Supports(agent.FeatureDevices) {
		t.Fatalf("after the rolled-back heartbeat the host has %v, want reserved and not mapping devices", got.Features)
	}
	if err := h.c.Reconcile(h.ctx); err != nil {
		t.Fatal(err)
	}
	h.c.lifecycleCalls.Wait()
	if h.hasTaskOfKind(emu.ID, agent.TaskCreateRunner) {
		t.Fatal("a general runner was placed on the reserved host after its agent stopped saying it was reserved")
	}
}

// Review r2 of #23, finding 1, GitHub's side: a runner of a pool that maps
// devices is registered only in a runner group GitHub restricts to selected
// workflows and keeps from public repositories, so GitHub cannot hand it an
// ordinary job. Otherwise no credential is minted and the runner fails.
func TestADeviceRunnerRegistersOnlyInAWorkflowRestrictedGroup(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(h *harness) string
		ok    bool
	}{
		{"restricted to workflows, private", func(h *harness) string { return h.emulatorGroup() }, true},
		{"not restricted to workflows", func(h *harness) string {
			h.gh.AddRunnerGroup("emulator")
			h.gh.SetRunnerGroupPublicAccess("emulator", false)
			return "emulator"
		}, false},
		{"open to public repositories", func(h *harness) string {
			h.gh.AddRunnerGroup("emulator")
			h.gh.SetRunnerGroupRestrictedToWorkflows("emulator", true)
			return "emulator"
		}, false},
		{"a group GitHub does not have", func(*harness) string { return "emulator" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			inst := h.installation()
			pool := h.pool(inst, "emulator")
			pool.MinRunners = 1
			pool.HostSelector = store.StringMap{"kvm": "ok"}
			pool.RunnerSettings.Devices = []string{"/dev/kvm"}
			pool.RunnerGroup = tc.setup(h)
			if err := h.st.UpdatePool(h.ctx, pool); err != nil {
				t.Fatal(err)
			}
			host := h.kvmHost("ci-nl-01-emu")
			if err := h.c.Reconcile(h.ctx); err != nil {
				t.Fatal(err)
			}
			h.c.lifecycleCalls.Wait()
			if got := h.hasTaskOfKind(host.ID, agent.TaskCreateRunner); got != tc.ok {
				t.Fatalf("create queued %v, want %v", got, tc.ok)
			}
			if !tc.ok {
				var failed bool
				for _, r := range h.runners() {
					failed = failed || (r.State == store.RunnerFailed && strings.Contains(r.Message, "restricted to selected workflows"))
				}
				if !failed {
					t.Fatalf("no runner failed saying why: %+v", h.runners())
				}
			}
		})
	}
}
