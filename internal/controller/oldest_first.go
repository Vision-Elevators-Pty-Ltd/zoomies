package controller

import (
	"context"
	"fmt"
	"strings"

	"github.com/eyupio/zoomies/internal/naming"
	"github.com/eyupio/zoomies/internal/scheduler"
	"github.com/eyupio/zoomies/internal/store"
)

// noteHolds writes oldest-first admission's decisions (scheduler.oldest_first)
// on the timelines of the jobs they concern: on an old job whose runner fits
// nowhere yet, which host's room is reserved for it; on the job a younger
// pool would have started next, which older job it is waiting behind. Each
// job gets one entry per change of sentence, not one per pass, and a job that
// is no longer queued is forgotten.
func (c *Controller) noteHolds(ctx context.Context, snap scheduler.Snapshot, plan scheduler.Plan) {
	if !c.mayAct() {
		return
	}
	queued := make(map[string]*store.Job, len(snap.Jobs))
	for _, j := range snap.Jobs {
		if j != nil {
			queued[j.ID] = j
		}
	}
	want := map[string]string{}
	for _, h := range plan.Holds {
		if h.Fits {
			continue
		}
		want[h.JobID] = fmt.Sprintf("capacity reserved on %s for this job: it is the oldest waiting job there and its runner (%s) fits on no host yet, so younger jobs start on %s only in what is left beside it",
			naming.ForSentence(h.HostName), scheduler.FormatMB(h.MemoryMB), naming.ForSentence(h.HostName))
	}
	for _, pp := range plan.Pools {
		if pp.WaitingFor == nil || pp.WaitingJobID == "" {
			continue
		}
		if _, held := want[pp.WaitingJobID]; !held {
			want[pp.WaitingJobID] = "waiting: " + pp.WaitingFor.Phrase(snap.Now)
		}
	}
	if c.holdNotes == nil {
		c.holdNotes = map[string]string{}
	}
	for id := range c.holdNotes {
		if queued[id] == nil {
			delete(c.holdNotes, id)
		}
	}
	for id, msg := range want {
		j := queued[id]
		if j == nil || sameHoldNote(c.holdNotes[id], msg) {
			continue
		}
		c.appendJobEvent(ctx, j, store.JobEventReserved, msg)
		c.holdNotes[id] = msg
	}
}

// sameHoldNote compares two sentences without the age they quote, which
// changes every pass while the decision does not.
func sameHoldNote(a, b string) bool {
	return trimQueuedAge(a) == trimQueuedAge(b)
}

func trimQueuedAge(s string) string {
	if i := strings.LastIndex(s, ", queued "); i >= 0 {
		return s[:i]
	}
	return s
}
