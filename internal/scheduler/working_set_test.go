package scheduler

import (
	"testing"

	"github.com/eyupio/zoomies/internal/store"
)

// Review of #20, blocker 1, the review's probe made permanent: five light runs
// of one job measured the new way must not drop a pool's old runs of a heavy
// job that has not been, so the booking of a runner that may yet take the
// heavy job is what it was. Before the fix the pool's booking fell from
// 10,240 MiB to 1,280 MiB.
func TestRemeasuredLightJobsDoNotLowerAPoolsBookingForAHeavyJob(t *testing.T) {
	var peaks []store.JobPeak
	for range 5 {
		peaks = append(peaks, store.JobPeak{Job: "light", MemoryMB: 1024, WorkingSetMB: 1024})
	}
	for range 20 {
		peaks = append(peaks, store.JobPeak{Job: "heavy", MemoryMB: 8192})
	}
	before, _ := historyBooking(peaks, 25)
	after, _ := historyBooking(store.PreferWorkingSet(peaks), 25)
	if after < before {
		t.Fatalf("the pool's booking fell from %d to %d MiB when only the light job was re-measured", before, after)
	}
	key := store.JobUsageKey{Repo: "org/repo", Workflow: "ci", JobName: "heavy", PoolID: "pool"}
	// The heavy job has fewer than five runs of its own; the pool's figure is
	// what protects a runner that may take it.
	jobHistory := map[store.JobUsageKey][]store.JobPeak{key: peaks[5:9]}
	pool := &store.Pool{ID: "pool"}
	demand := map[string][]*store.Job{"pool": {{Repo: key.Repo, Workflow: key.Workflow, JobName: key.JobName}}}
	b := newBooker(BookingHistory, 25, &BookingInputs{PoolRuns: map[string][]store.JobPeak{"pool": store.PreferWorkingSet(peaks)}},
		jobHistory, []*store.Pool{pool}, demand)
	if b.unknown["pool"] < before {
		t.Fatalf("a runner that may take the queued heavy job books %d MiB, below the %d it booked before", b.unknown["pool"], before)
	}
	// A killed run survives the switch with its floor.
	peaks = append(peaks, store.JobPeak{Job: "heavy", MemoryMB: 8192, OOMKilled: true, GrantedMemoryMB: 8192})
	if got, _ := historyBooking(store.PreferWorkingSet(peaks), 25); got < before {
		t.Fatalf("the killed run's floor was lost: %d MiB", got)
	}
}

// Review of #20, blocker 2, the review's probe made permanent: a job whose new
// measurements are partial (readings with a gap: untrusted, -1) keeps the
// raise its old runs give it. Before the fix five such runs at 512 MiB took
// an 8 GiB job's raise need from 10,240 MiB to 640 MiB.
func TestPartialMeasurementsDoNotLowerAJobsRaise(t *testing.T) {
	var peaks []store.JobPeak
	for range 5 {
		peaks = append(peaks, store.JobPeak{MemoryMB: 8192, WorkingSetMB: -1})
	}
	for range 15 {
		peaks = append(peaks, store.JobPeak{MemoryMB: 8192})
	}
	before, _ := raiseNeedMB(peaks, 25)
	after, _ := raiseNeedMB(store.PreferWorkingSet(peaks), 25)
	if before != 10240 || after != before {
		t.Fatalf("raise need %d MiB before, %d after; want 10,240 both", before, after)
	}
	if b, _ := historyBooking(store.PreferWorkingSet(peaks), 25); b < 10240 {
		t.Fatalf("booking fell to %d MiB on partial measurements", b)
	}
}
