package controller

import (
	"strings"
	"testing"

	"github.com/eyupio/zoomies/internal/agent"
	"github.com/eyupio/zoomies/internal/config"
	"github.com/eyupio/zoomies/internal/scheduler"
	"github.com/eyupio/zoomies/internal/store"
)

// A runner whose job the last pass found heavier than it is given a valve rule
// to grow into it, on a pool whose own valve is off: finding 70's typecheck,
// killed at its typed size by a job whose history says it needs more.
func TestARunnerWhoseJobOutgrowsItIsLentTheDifferenceWithThePoolsValveOff(t *testing.T) {
	h := newHarness(t)
	_, pool, host := h.fleet()
	h.valvePool(pool, store.MemoryBurstOff, 0)
	heavy := h.valveRunner(pool, host, store.RunnerBusy)
	light := h.valveRunner(pool, host, store.RunnerBusy)
	h.c.setLastPlan(scheduler.Plan{RunnerNeeds: map[string]int64{heavy.ID: 6144}})

	got := memoryBeat(h, host)
	rule, ok := ruleFor(got, heavy.ID)
	if !ok || rule.Mode != store.MemoryBurstAutomatic || rule.GuaranteeMB != 4096 || rule.CeilingMB != 6144 {
		t.Fatalf("rule = %+v (%v), want automatic from 4096 up to the job's 6144", rule, ok)
	}
	if _, ok := ruleFor(got, light.ID); ok {
		t.Fatal("a runner whose job fits it was given a rule on a pool with the valve off")
	}
	// The grant is added on top of what the two guarantees leave above the
	// floor, so the agent's own check against the capacity allows it.
	host, _ = h.st.GetHost(h.ctx, host.ID)
	floor := scheduler.MemoryPoolFloor(host)
	if want := host.MemoryMB - floor - 2*4096 + 2048; got.CapacityMB != want {
		t.Fatalf("capacity = %d, want the machine above its floor less both guarantees and plus the 2 GB grant, %d", got.CapacityMB, want)
	}
	if s := h.c.memoryState(host.ID); s.History != 1 || s.Enforcing != 1 || s.Pool.HistoryGrantMB != 2048 {
		t.Fatalf("state = %+v, want one history grant of 2048 MB counted as enforcing", s)
	}
	if v := h.c.memoryPoolView(host); v == nil || v.History != 1 || v.HistoryGrantMB != 2048 {
		t.Fatalf("host memory pool view = %+v, want the grant", v)
	}
}

// Off is no grant, so a host whose pools have the valve off is sent
// nothing, the same heartbeat after the setting changes.
func TestHistoryRaiseOffSendsNoGrant(t *testing.T) {
	h := newHarness(t)
	_, pool, host := h.fleet()
	h.valvePool(pool, store.MemoryBurstOff, 0)
	heavy := h.valveRunner(pool, host, store.RunnerBusy)
	h.c.setLastPlan(scheduler.Plan{RunnerNeeds: map[string]int64{heavy.ID: 6144}})
	h.c.UpdateConfig(func(c *config.Config) { c.Scheduler.HistoryRaise = scheduler.HistoryRaiseOff })

	if got := memoryBeat(h, host); got != nil {
		t.Fatalf("directive = %+v, want none with history_raise off", got)
	}
	if v := h.c.runnerView(h.ctx, heavy).MemoryResource; v != nil {
		t.Fatalf("with history_raise off the runner's memory view is %+v, want none", v)
	}
}

// A runner a pool's own valve already lends to keeps its rule, with the larger
// ceiling of the two.
func TestAGrantOnAPoolWithTheValveOnTakesTheLargerCeiling(t *testing.T) {
	h := newHarness(t)
	_, pool, host := h.fleet()
	h.valvePool(pool, store.MemoryBurstAutomatic, 1024)
	heavy := h.valveRunner(pool, host, store.RunnerBusy)
	modest := h.valveRunner(pool, host, store.RunnerBusy)
	// 7168 is above the pool's own ceiling of 6144; 5120 is below it.
	h.c.setLastPlan(scheduler.Plan{RunnerNeeds: map[string]int64{heavy.ID: 7168, modest.ID: 5120}})

	got := memoryBeat(h, host)
	if rule, _ := ruleFor(got, heavy.ID); rule.CeilingMB != 7168 || rule.SpillMB != 1024 || rule.Mode != store.MemoryBurstAutomatic {
		t.Fatalf("heavy rule = %+v, want the job's 7168 with the pool's swap", rule)
	}
	if rule, _ := ruleFor(got, modest.ID); rule.CeilingMB != 6144 {
		t.Fatalf("modest rule = %+v, want the pool's own 6144", rule)
	}
	if s := h.c.memoryState(host.ID); s.History != 1 || s.Pool.HistoryGrantMB != 1024 {
		t.Fatalf("state = %+v, want one grant adding 1024 MB above the pool's ceiling", s)
	}
}

// A pair held to one limit is not given a grant: its slice would cap what the
// valve raised. It is raised when it is created instead.
func TestAPairHeldToOneLimitIsNotLentForItsJob(t *testing.T) {
	h := newHarness(t)
	_, pool, host := h.fleet()
	h.valvePool(pool, store.MemoryBurstOff, 0)
	pool.DockerMode = store.DockerDinD
	pool.Resources.DinDSharedLimit = true
	if err := h.st.UpdatePool(h.ctx, pool); err != nil {
		t.Fatal(err)
	}
	heavy := h.valveRunner(pool, host, store.RunnerBusy)
	h.c.setLastPlan(scheduler.Plan{RunnerNeeds: map[string]int64{heavy.ID: 6144}})
	resp, err := h.c.Heartbeat(h.ctx, host.ID, agent.HeartbeatRequest{
		Features: []string{agent.FeatureElasticMemory, agent.FeatureDinDSharedLimit},
	})
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if resp.ElasticMemory != nil {
		t.Fatalf("directive = %+v, want none for a shared-limit pair", resp.ElasticMemory)
	}
}

// The Runners page says why a runner may hold more than its pool's size.
func TestARunnersMemoryViewSaysItWasRaisedForItsJobsHistory(t *testing.T) {
	h := newHarness(t)
	_, pool, host := h.fleet()
	h.valvePool(pool, store.MemoryBurstOff, 0)
	heavy := h.valveRunner(pool, host, store.RunnerBusy)
	h.c.setLastPlan(scheduler.Plan{RunnerNeeds: map[string]int64{heavy.ID: 6144}})

	v := h.c.runnerView(h.ctx, heavy).MemoryResource
	if v == nil || v.HistoryNeedMB != 6144 || v.CeilingMB != 6144 || v.Mode != "automatic" {
		t.Fatalf("memory view = %+v, want the job's need of 6144 as its ceiling", v)
	}
	if !strings.Contains(v.Reason, "raised for job history (need 6144 MB)") {
		t.Fatalf("reason %q does not say why", v.Reason)
	}
}

// The pass reads which job each runner is running whenever history_raise is on,
// not only where memory is booked by history: a fleet at 100% still raises a
// runner whose job outgrows it.
func TestThePassReadsRunningJobsForHistoryRaiseWithoutOvercommit(t *testing.T) {
	h := newHarness(t)
	h.fleet()
	s, err := h.c.snapshot(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if s.Booking == nil || s.Policy.HistoryRaise != scheduler.HistoryRaiseOn {
		t.Fatalf("with history_raise on by default the pass has booking %+v and policy %q", s.Booking, s.Policy.HistoryRaise)
	}
	h.c.UpdateConfig(func(c *config.Config) { c.Scheduler.HistoryRaise = scheduler.HistoryRaiseOff })
	if s, _ = h.c.snapshot(h.ctx); s.Booking != nil {
		t.Fatalf("with history_raise off and memory booked once the pass read %+v", s.Booking)
	}
}

// A pool that only observes does not turn a grant into an observation: the
// runner whose job outgrows it gets an automatic rule up to the grant, not the
// pool's larger observing ceiling, which nobody asked to lend up to.
func TestAGrantOnAnObservingPoolIsCarriedOut(t *testing.T) {
	h := newHarness(t)
	_, pool, host := h.fleet()
	h.valvePool(pool, store.MemoryBurstObserve, 0)
	heavy := h.valveRunner(pool, host, store.RunnerBusy)
	watched := h.valveRunner(pool, host, store.RunnerBusy)
	// The pool's own ceiling is 6144; the grant's 5120 is below it.
	h.c.setLastPlan(scheduler.Plan{RunnerNeeds: map[string]int64{heavy.ID: 5120}})

	got := memoryBeat(h, host)
	if rule, _ := ruleFor(got, heavy.ID); rule.Mode != store.MemoryBurstAutomatic || rule.CeilingMB != 5120 {
		t.Fatalf("heavy rule = %+v, want automatic up to the grant's 5120", rule)
	}
	if rule, _ := ruleFor(got, watched.ID); rule.Mode != store.MemoryBurstObserve || rule.CeilingMB != 6144 {
		t.Fatalf("watched rule = %+v, want the pool's observing rule", rule)
	}
	if s := h.c.memoryState(host.ID); s.History != 1 || s.Enforcing != 1 || s.Observing != 1 || s.Pool.HistoryGrantMB != 1024 {
		t.Fatalf("state = %+v, want one grant of 1024 MB from its guarantee", s)
	}
}

// A pass that could not read which job each runner runs, or the jobs' history,
// keeps the grants the last pass worked out; a pass that read them and found
// none replaces them.
func TestAPassThatCouldNotReadHistoryKeepsTheLastGrants(t *testing.T) {
	h := newHarness(t)
	_, pool, host := h.fleet()
	h.valvePool(pool, store.MemoryBurstOff, 0)
	heavy := h.valveRunner(pool, host, store.RunnerBusy)
	h.c.setLastPlan(scheduler.Plan{RunnerNeeds: map[string]int64{heavy.ID: 6144}})
	h.c.setLastPlan(scheduler.Plan{})
	if _, ok := ruleFor(memoryBeat(h, host), heavy.ID); !ok {
		t.Fatal("a plan that knew nothing of the running jobs dropped the grant")
	}
	h.c.setLastPlan(scheduler.Plan{RunnerNeeds: map[string]int64{}})
	if got := memoryBeat(h, host); got != nil {
		t.Fatalf("a plan that found no runner needing more kept %+v", got)
	}
}

// The scheduler names the agent's lending feature without importing the agent,
// and books a raised runner only on a host that advertises it.
func TestTheSchedulersElasticMemoryFeatureIsTheAgents(t *testing.T) {
	if scheduler.ElasticMemoryFeature != agent.FeatureElasticMemory {
		t.Fatalf("scheduler.ElasticMemoryFeature = %q, agent.FeatureElasticMemory = %q", scheduler.ElasticMemoryFeature, agent.FeatureElasticMemory)
	}
}
