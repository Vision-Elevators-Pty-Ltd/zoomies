package controller

import (
	"time"

	"github.com/eyupio/zoomies/internal/store"
)

// heldToArrival holds the times GitHub sent about a job to the moment the
// controller received them.
//
// GitHub's workflow_job timestamps are its own clock, and that clock has been
// seen running most of an hour ahead of ours. A job stored as queued in the
// future has, by every age the scheduler computes, not waited at all, so a pool
// with a scale-up delay never scales for it and the queue stalls behind it.
// Nothing GitHub reports can have happened after we were told of it, so a later
// time is read as now. An earlier one is kept exactly as sent: it is the only
// record of how long the job really waited before we heard.
func heldToArrival(j *store.Job, now time.Time) {
	if j.QueuedAt.After(now) {
		j.QueuedAt = now
	}
	for _, t := range []**time.Time{&j.StartedAt, &j.CompletedAt, &j.EligibleAt} {
		if *t != nil && (*t).After(now) {
			at := now
			*t = &at
		}
	}
}
