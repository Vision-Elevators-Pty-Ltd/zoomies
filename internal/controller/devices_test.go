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

// kvmHost is a host labelled kvm=ok whose agent maps devices.
func (h *harness) kvmHost(name string) *store.Host {
	h.t.Helper()
	host := h.measuredHost(name, 8, 32768, 4, enforcesEverything)
	labels := store.StringMap{"kvm": "ok"}
	if err := h.st.PatchHost(h.ctx, host.ID, store.HostChanges{Labels: &labels}); err != nil {
		h.t.Fatalf("PatchHost: %v", err)
	}
	host.Features = append(host.Features, agent.FeatureDevices)
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
			host.Features = store.StringSlice{agent.FeatureDaemonShare}
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
	emu.Features = append(emu.Features, agent.FeatureDevicesOnly)
	if err := h.st.SetHostReported(h.ctx, emu); err != nil {
		t.Fatal(err)
	}
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
