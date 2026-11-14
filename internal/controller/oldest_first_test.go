package controller

import (
	"strings"
	"testing"
	"time"

	"github.com/eyupio/zoomies/internal/scheduler"
	"github.com/eyupio/zoomies/internal/store"
)

// Oldest-first admission is on by default, and the policy every pass is
// decided under carries it.
func TestOldestFirstIsOnByDefault(t *testing.T) {
	h := newHarness(t)
	if got := h.c.cfg().Scheduler.OldestFirst; got != scheduler.OldestFirstOn {
		t.Fatalf("scheduler.oldest_first default = %q, want on", got)
	}
}

// Both sides of a hold reach the timelines: the old job is told its room is
// reserved, the younger job which older job it waits behind. A decision that
// stands for many passes is written once, however its quoted age moves, and a
// changed decision is written again.
func TestHoldsAreWrittenOnceOnTheTimelinesOfBothJobs(t *testing.T) {
	h := newHarness(t)
	_, pool, _ := h.fleet()
	queuedAt := h.c.Now().Add(-10 * time.Minute)
	upsert := func(id int64, name string, at time.Time) *store.Job {
		j, err := h.st.UpsertJob(h.ctx, &store.Job{GitHubJobID: id, Repo: "eyupio/zoomies", Workflow: "CI", JobName: name,
			Labels: pool.Labels, State: store.JobQueued, PoolID: pool.ID, Matched: true, QueuedAt: at})
		if err != nil {
			t.Fatalf("UpsertJob: %v", err)
		}
		return j
	}
	old := upsert(1, "visual-build", queuedAt)
	young := upsert(2, "lint", queuedAt.Add(5*time.Minute))
	hold := scheduler.Hold{JobID: old.ID, JobName: old.JobName, QueuedAt: old.QueuedAt, PoolID: "pool_big", PoolName: "big",
		HostID: "host_a", HostName: "ci-nl-02-trusted", MemoryMB: 24 * 1024}
	plan := scheduler.Plan{Holds: []scheduler.Hold{hold},
		Pools: []scheduler.PoolPlan{{PoolID: "pool_small", WaitingFor: &hold, WaitingJobID: young.ID}}}
	snap := scheduler.Snapshot{Now: h.c.Now(), Jobs: []*store.Job{old, young}}

	reserved := func(id string) []*store.JobEvent {
		events, err := h.c.JobEvents(h.ctx, id)
		if err != nil {
			t.Fatalf("JobEvents: %v", err)
		}
		var out []*store.JobEvent
		for _, e := range events {
			if e.Kind == store.JobEventReserved {
				out = append(out, e)
			}
		}
		return out
	}

	h.c.noteHolds(h.ctx, snap, plan)
	snap.Now = snap.Now.Add(time.Minute) // the age quoted moves; the decision does not
	h.c.noteHolds(h.ctx, snap, plan)

	o, y := reserved(old.ID), reserved(young.ID)
	if len(o) != 1 || !strings.Contains(o[0].Message, "capacity reserved on ci-nl-02-trusted for this job") {
		t.Fatalf("old job's reserved entries = %+v, want one naming its host", o)
	}
	if len(y) != 1 || !strings.Contains(y[0].Message, "waiting: capacity on ci-nl-02-trusted reserved for older job "+old.ID) {
		t.Fatalf("young job's reserved entries = %+v, want one naming the older job", y)
	}

	// The reservation moves to another host: said again, once.
	hold.HostName = "ci-nl-03-trusted"
	plan.Holds[0] = hold
	plan.Pools[0].WaitingFor = &hold
	h.c.noteHolds(h.ctx, snap, plan)
	if got := reserved(young.ID); len(got) != 2 || !strings.Contains(got[1].Message, "ci-nl-03-trusted") {
		t.Fatalf("young job's entries after the hold moved = %+v, want a second naming the new host", got)
	}
}
