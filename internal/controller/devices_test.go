package controller

import (
	"testing"

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
	kvm := h.measuredHost("ci-nl-02", 8, 32768, 4, enforcesEverything)
	labels := store.StringMap{"kvm": "ok"}
	if err := h.st.PatchHost(h.ctx, kvm.ID, store.HostChanges{Labels: &labels}); err != nil {
		t.Fatalf("PatchHost: %v", err)
	}
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
