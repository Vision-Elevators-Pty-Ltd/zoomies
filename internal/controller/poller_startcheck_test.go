package controller

import (
	"strings"
	"testing"
	"time"

	"github.com/eyupio/zoomies/internal/store"
)

// jobLookups counts the GetWorkflowJob calls made since a point in the fake's
// request log: one each, and what every known-job check costs the quota.
func (h *harness) jobLookups(since int) int {
	h.t.Helper()
	n := 0
	for _, req := range h.gh.Requests()[since:] {
		if strings.Contains(req, "/actions/jobs/") {
			n++
		}
	}
	return n
}

func (h *harness) queuedDemand() []*store.Job {
	h.t.Helper()
	queued, err := h.st.ListQueuedJobs(h.ctx)
	if err != nil {
		h.t.Fatalf("ListQueuedJobs: %v", err)
	}
	return queued
}

// Measured on 4 November 2026 on a polling-only controller: job 126527192046
// started on GitHub 26 s after it was queued and had finished, and the
// controller still counted it as queued at 128 s, having started seven runners
// for it. A short job is over before the next sweep, so it is in no listing,
// and the known-job rotation does not look at a job until it is two minutes
// old. The sweep that no longer finds the job queued has to get it checked on
// the next recovery pass, well inside those two minutes.
func TestAJobThatStartedAndFinishedBetweenSweepsStopsBeingDemandInsideTwoMinutes(t *testing.T) {
	h := newHarness(t)
	h.fleet()
	q := h.gh.AddQueuedJob("acme/widgets", "CI", "lint", demoLabels)
	h.c.discoverJobs(h.ctx)
	if job := h.polledJob(q.ID); job.State != store.JobQueued {
		t.Fatalf("job = %s, want it queued before anything took it", job.State)
	}

	// Taken by a runner this controller does not know (a static one) and
	// finished, all between two sweeps.
	h.gh.StartJob(q.ID, "static-runner-1")
	h.gh.CompleteJob(q.ID, "success")
	h.advance(20 * time.Second)

	before := len(h.gh.Requests())
	h.c.discoverJobs(h.ctx)
	if got := h.jobLookups(before); got != 0 {
		t.Fatalf("the sweep made %d job lookups of its own; the check belongs to the recovery loop's batch", got)
	}
	h.c.reconcileKnownJobs(h.ctx, h.c.Now())

	if got := h.jobLookups(before); got != 1 {
		t.Fatalf("the recovery pass made %d job lookups, want the one for the missing job", got)
	}
	job := h.polledJob(q.ID)
	if job.State != store.JobCompleted || job.Conclusion != "success" {
		t.Fatalf("job = %s (%q) 20 s after it was queued, want GitHub's completion", job.State, job.Conclusion)
	}
	if queued := h.queuedDemand(); len(queued) != 0 {
		t.Fatalf("a finished job still creates demand: %+v", queued)
	}
	// Checked once: the next pass has nothing to look up.
	before = len(h.gh.Requests())
	h.c.discoverJobs(h.ctx)
	h.c.reconcileKnownJobs(h.ctx, h.c.Now())
	if got := h.jobLookups(before); got != 0 {
		t.Fatalf("a job already settled was looked up %d more times", got)
	}
}

// Most of the queued jobs the controller listed on 4 November were running on
// GitHub, some for over a minute. A job still on the listing as running costs
// nothing to see, and a known queued row has to be started from it whoever's
// runner took it: waiting for the known-job check left it as demand.
func TestASweepStartsAKnownQueuedJobRunningOnARunnerElsewhere(t *testing.T) {
	h := newHarness(t)
	h.fleet()
	q := h.gh.AddQueuedJob("acme/widgets", "CI", "build", demoLabels)
	h.c.discoverJobs(h.ctx)

	h.gh.StartJob(q.ID, "static-runner-1")
	h.advance(15 * time.Second)
	before := len(h.gh.Requests())
	h.c.discoverJobs(h.ctx)

	job := h.polledJob(q.ID)
	if job.State != store.JobInProgress || job.StartedAt == nil {
		t.Fatalf("job = %s, want it in progress from the sweep's own listing", job.State)
	}
	if job.RunnerID != "" {
		t.Fatalf("job linked to runner %q, want no runner of this fleet", job.RunnerID)
	}
	if got := h.jobLookups(before); got != 0 {
		t.Fatalf("starting a listed running job cost %d lookups", got)
	}
	if queued := h.queuedDemand(); len(queued) != 0 {
		t.Fatalf("a started job still creates demand: %+v", queued)
	}
	if got := len(h.runners()); got != 0 {
		t.Fatalf("the sweep created %d fleet runners for work running elsewhere", got)
	}
}

// The checks come out of the recovery pass's batch rather than on top of it:
// the pass costs at most reconcileBatch lookups however many jobs went
// missing, and the rotation keeps at least what the start checks leave it.
func TestStartChecksStayInsideTheRecoveryBatch(t *testing.T) {
	h := newHarness(t)
	h.fleet()
	var ids []int64
	for i := 0; i < 15; i++ {
		ids = append(ids, h.gh.AddQueuedJob("acme/widgets", "CI", "shard", demoLabels).ID)
	}
	h.c.discoverJobs(h.ctx)
	if got := len(h.queuedDemand()); got != 15 {
		t.Fatalf("setup: %d queued, want 15", got)
	}
	for _, id := range ids {
		h.gh.StartJob(id, "static-runner-1")
		h.gh.CompleteJob(id, "success")
	}
	// Old enough for the rotation too, so both halves of the batch have work.
	h.advance(3 * time.Minute)
	h.c.discoverJobs(h.ctx)

	for pass := 0; pass < 3; pass++ {
		before := len(h.gh.Requests())
		h.c.reconcileKnownJobs(h.ctx, h.c.Now())
		got := h.jobLookups(before)
		if got > reconcileBatch {
			t.Fatalf("pass %d made %d lookups, more than the batch of %d", pass, got, reconcileBatch)
		}
		if pass == 0 && got < startCheckShare {
			t.Fatalf("pass 0 made %d lookups, want at least the %d start checks", got, startCheckShare)
		}
	}
	if queued := h.queuedDemand(); len(queued) != 0 {
		t.Fatalf("%d finished jobs still create demand after three passes", len(queued))
	}
}

// The listing is bounded, so on a busy organisation a job that really is still
// queued can be missing from every sweep. It is checked once, then left for
// startRecheckAfter, so it cannot take the share on every pass.
func TestAJobStillQueuedIsNotRecheckedOnEveryPass(t *testing.T) {
	h := newHarness(t)
	inst, _, _ := h.fleet()
	q := h.gh.AddQueuedJob("acme/widgets", "CI", "build", demoLabels)
	h.c.discoverJobs(h.ctx)
	swept := map[string]bool{inst.ID: true}

	// A sweep that did not reach the job's run.
	missing := func() int {
		h.t.Helper()
		before := len(h.gh.Requests())
		h.c.noteUnlistedQueuedJobs(h.ctx, h.c.Now(), map[int64]bool{}, swept)
		h.c.reconcileKnownJobs(h.ctx, h.c.Now())
		return h.jobLookups(before)
	}
	if got := missing(); got != 1 {
		t.Fatalf("the first absence cost %d lookups, want 1", got)
	}
	if job := h.polledJob(q.ID); job.State != store.JobQueued {
		t.Fatalf("job = %s, want it left queued as GitHub has it", job.State)
	}
	h.advance(15 * time.Second)
	if got := missing(); got != 0 {
		t.Fatalf("a job checked 15 s ago was looked up again (%d)", got)
	}
	h.advance(startRecheckAfter)
	if got := missing(); got != 1 {
		t.Fatalf("after %s the job cost %d lookups, want it checked again", startRecheckAfter, got)
	}
}

// An installation inside its rate-limit hold is not called, by a start check
// any more than by the rotation, and its jobs wait on the list for the hold to
// end rather than being dropped from it.
func TestStartChecksRespectTheRateLimitHold(t *testing.T) {
	h := newHarness(t)
	inst, _, _ := h.fleet()
	q := h.gh.AddQueuedJob("acme/widgets", "CI", "build", demoLabels)
	h.c.discoverJobs(h.ctx)
	h.gh.StartJob(q.ID, "static-runner-1")
	h.gh.CompleteJob(q.ID, "success")
	h.c.discoverJobs(h.ctx)

	h.c.holdGitHub(inst.ID, h.c.Now().Add(time.Minute))
	before := len(h.gh.Requests())
	h.c.reconcileKnownJobs(h.ctx, h.c.Now())
	if got := h.jobLookups(before); got != 0 {
		t.Fatalf("a held installation was called %d times", got)
	}
	if job := h.polledJob(q.ID); job.State != store.JobQueued {
		t.Fatalf("job = %s, want it untouched while the installation is held", job.State)
	}

	h.advance(time.Minute + time.Second)
	h.c.reconcileKnownJobs(h.ctx, h.c.Now())
	if job := h.polledJob(q.ID); job.State != store.JobCompleted {
		t.Fatalf("job = %s after the hold, want the check it waited for to complete it", job.State)
	}
}
