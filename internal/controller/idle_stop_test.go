package controller

import (
	"testing"
	"time"

	"github.com/eyupio/zoomies/internal/agent"
	"github.com/eyupio/zoomies/internal/store"
)

func TestAnUnconfirmedIdleStopKeepsItsCapacityAcrossFailuresAndRestarts(t *testing.T) {
	for _, tc := range []struct {
		name    string
		restart bool
		ok      bool
		state   store.RunnerState
	}{
		{"worker present", false, false, ""},
		{"probe failed", false, false, store.RunnerFailed},
		{"stop failed", false, false, ""},
		{"stop accepted without an exit", false, true, ""},
		{"unexpected successful drain claim", false, true, store.RunnerDraining},
		{"unexpected successful failure claim", false, true, store.RunnerFailed},
		{"late refusal after restart", true, false, store.RunnerFailed},
		{"late stop acceptance after restart", true, true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			_, pool, host := h.fleet()
			pool.Enabled = false
			pool.Resources = store.Resources{CPUs: 2, MemoryMB: 4096}
			if err := h.st.UpdatePool(h.ctx, pool); err != nil {
				t.Fatal(err)
			}
			r := h.runnerRow(pool, host, store.RunnerIdle)
			if _, err := h.c.drainRunner(h.ctx, r, "idle surplus", pool, true); err != nil {
				t.Fatal(err)
			}
			task := h.taskOfKind(host.ID, agent.TaskStopIdleRunner)
			if tc.restart {
				h.c = h.restart()
			}
			if err := h.c.ReportResult(h.ctx, host.ID, agent.TaskResult{
				TaskID: task.ID, Kind: task.Kind, RunnerID: r.ID,
				OK: tc.ok, State: tc.state, Error: "idle stop not confirmed",
			}); err != nil {
				t.Fatal(err)
			}

			// A drain timeout used to free the booking and queue removal after
			// a refused stop, even though the workload could still be busy.
			h.advance(time.Hour)
			host.LastHeartbeat = h.c.Now()
			if err := h.st.UpdateHost(h.ctx, host); err != nil {
				t.Fatal(err)
			}
			if err := h.c.Reconcile(h.ctx); err != nil {
				t.Fatal(err)
			}
			after := h.runnerByID(t, r.ID)
			if after.State != store.RunnerIdle {
				t.Fatalf("unconfirmed stop changed state to %s", after.State)
			}
			current, err := h.st.GetHost(h.ctx, host.ID)
			if err != nil {
				t.Fatal(err)
			}
			view := h.c.HostView(current)
			if current.ActiveRunners != 1 || view.ReservedCPUs != 2 || view.ReservedMemoryMB != 4096 {
				t.Fatalf("booking was released: active %d, CPUs %v, memory %d", current.ActiveRunners, view.ReservedCPUs, view.ReservedMemoryMB)
			}
			if h.hasTaskOfKind(host.ID, agent.TaskRemoveRunner) {
				t.Fatal("an unconfirmed idle stop queued removal")
			}
		})
	}
}

func TestAnIdleStopWithNoResultKeepsItsBookingAfterEveryDeliveryExpires(t *testing.T) {
	h := newHarness(t)
	_, pool, host := h.fleet()
	r := h.runnerRow(pool, host, store.RunnerIdle)
	if _, err := h.c.drainRunner(h.ctx, r, "idle surplus", pool, true); err != nil {
		t.Fatal(err)
	}
	q := h.c.queues.get(host.ID)
	now := h.c.Now()
	for attempt := 0; attempt < maxTaskAttempts; attempt++ {
		if tasks := q.take(10, now); len(tasks) != 1 || tasks[0].Kind != agent.TaskStopIdleRunner {
			t.Fatalf("delivery %d = %+v", attempt, tasks)
		}
		now = now.Add(stopLease + time.Minute)
		h.c.sweepTasks(h.ctx, now)
	}
	if pending, inflight := q.depth(); pending != 0 || inflight != 0 {
		t.Fatalf("exhausted task remains %d/%d", pending, inflight)
	}
	if after := h.runnerByID(t, r.ID); after.State != store.RunnerIdle {
		t.Fatalf("lease exhaustion changed state to %s", after.State)
	}
	current, err := h.st.GetHost(h.ctx, host.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.ActiveRunners != 1 {
		t.Fatal("lease exhaustion released the workload's slot")
	}
	if _, err := h.c.drainRunner(h.ctx, h.runnerByID(t, r.ID), "retry idle stop", pool, true); err != nil {
		t.Fatal(err)
	}
	if !h.hasTaskOfKind(host.ID, agent.TaskStopIdleRunner) {
		t.Fatal("the refused idle stop cannot be retried")
	}
}

func TestAnOlderAgentIsNeverAskedToWithdrawOrStopAnIdleRunner(t *testing.T) {
	h := newHarness(t)
	_, pool, host := h.fleet()
	host.Features = nil
	if err := h.st.SetHostReported(h.ctx, host); err != nil {
		t.Fatal(err)
	}
	r := h.runnerRow(pool, host, store.RunnerIdle)
	h.gh.AddRunner(r.Name, pool.Labels)
	before := len(h.gh.Requests())
	if _, err := h.c.drainRunner(h.ctx, r, "idle surplus", pool, true); err == nil {
		t.Fatal("older agent accepted a guarded stop")
	}
	if len(h.gh.Requests()) != before || !h.registered(r.Name) {
		t.Fatal("registration was touched before checking the agent capability")
	}
	if tasks := h.tasksFor(host.ID); len(tasks) != 0 {
		t.Fatalf("older agent was given tasks: %+v", tasks)
	}
	if after := h.runnerByID(t, r.ID); after.State != store.RunnerIdle {
		t.Fatalf("older agent changed runner to %s", after.State)
	}
}

func TestAConfirmedIdleWorkloadExitReleasesItsBooking(t *testing.T) {
	h := newHarness(t)
	_, pool, host := h.fleet()
	r := h.runnerRow(pool, host, store.RunnerIdle)
	if _, err := h.c.drainRunner(h.ctx, r, "idle surplus", pool, true); err != nil {
		t.Fatal(err)
	}
	task := h.taskOfKind(host.ID, agent.TaskStopIdleRunner)
	if err := h.c.ReportResult(h.ctx, host.ID, agent.TaskResult{TaskID: task.ID, Kind: task.Kind, RunnerID: r.ID, OK: true, State: store.RunnerRemoved}); err != nil {
		t.Fatal(err)
	}
	current, err := h.st.GetHost(h.ctx, host.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.ActiveRunners != 0 || h.runnerByID(t, r.ID).State != store.RunnerRemoved {
		t.Fatal("an actual workload exit did not release the booking")
	}
}

func TestAnIdleStopReturnedUnstartedNeverGivesUpItsBooking(t *testing.T) {
	h := newHarness(t)
	_, pool, host := h.fleet()
	r := h.runnerRow(pool, host, store.RunnerIdle)
	if _, err := h.c.drainRunner(h.ctx, r, "idle surplus", pool, true); err != nil {
		t.Fatal(err)
	}
	q := h.c.queues.get(host.ID)
	for attempt := 0; attempt < maxTaskAttempts; attempt++ {
		tasks := q.take(10, h.c.Now())
		if len(tasks) != 1 {
			t.Fatalf("delivery %d has %d tasks, want one", attempt, len(tasks))
		}
		if err := h.c.ReportResult(h.ctx, host.ID, agent.TaskResult{
			TaskID: tasks[0].ID, Kind: tasks[0].Kind, RunnerID: r.ID,
			NotStarted: true, OK: false, Error: "agent restarting",
		}); err != nil {
			t.Fatal(err)
		}
	}
	if pending, inflight := q.depth(); pending != 0 || inflight != 0 {
		t.Fatalf("exhausted task remains %d/%d", pending, inflight)
	}
	if after := h.runnerByID(t, r.ID); after.State != store.RunnerIdle {
		t.Fatalf("an unstarted stop changed state to %s", after.State)
	}
}
