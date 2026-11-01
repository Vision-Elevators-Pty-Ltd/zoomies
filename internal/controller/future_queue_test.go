package controller

import (
	"testing"
	"time"

	"github.com/eyupio/zoomies/internal/store"
)

// GitHub's workflow_job timestamps have been seen most of an hour ahead of real
// time. A job stored as queued in the future reads as not yet waiting, and its
// pool never scales for it; so what GitHub says is held to when we heard it,
// on both of the paths a job arrives by.

func TestAWebhookJobQueuedInTheFutureIsStoredAsQueuedWhenItArrived(t *testing.T) {
	h := newHarness(t)
	h.fleet()
	before := h.c.Now()
	h.deliverJob(jobEvent{Action: "queued", JobID: 7701, RunID: 8801, Repo: "acme/widgets", Name: "build",
		Workflow: "CI", Labels: []string{"self-hosted", "linux", "x64", "demo"}, QueuedAt: before.Add(48 * time.Minute)})

	job, err := h.st.GetJobByGitHubID(h.ctx, 7701)
	if err != nil {
		t.Fatal(err)
	}
	if job.QueuedAt.After(h.c.Now()) || job.QueuedAt.Before(before.Add(-time.Second)) {
		t.Fatalf("queued_at = %s; want it held to the moment the delivery arrived (%s)", job.QueuedAt, before)
	}
	if err := h.c.Reconcile(h.ctx); err != nil {
		t.Fatal(err)
	}
	if got := len(h.runners()); got != 1 {
		t.Fatalf("created %d runners for a job GitHub dated in the future, want 1", got)
	}
}

func TestAPolledJobQueuedInTheFutureIsStoredAsQueuedWhenItArrived(t *testing.T) {
	h := newHarness(t)
	h.fleet()
	q := h.gh.AddQueuedJob("acme/widgets", "CI", "build", []string{"self-hosted", "linux", "x64", "demo"})
	before := h.c.Now()
	h.gh.SetJobQueuedAt(q.ID, before.Add(48*time.Minute))

	h.c.pollOnce(h.ctx)

	job, err := h.st.GetJobByGitHubID(h.ctx, q.ID)
	if err != nil {
		t.Fatal(err)
	}
	if job.State != store.JobQueued || job.QueuedAt.After(h.c.Now()) {
		t.Fatalf("job = %s queued at %s; want it queued no later than now", job.State, job.QueuedAt)
	}
}
