package agent

import (
	"testing"
	"time"

	"github.com/eyupio/zoomies/internal/backend"
)

// A stats sample reports the most of its own working set and the fast
// readings of the window before it; older readings are dropped.
func TestAStatsSampleCarriesTheWindowsPeakWorkingSet(t *testing.T) {
	now := time.Date(2026, 11, 8, 8, 0, 0, 0, time.UTC)
	fast := []workingSetSample{
		{at: now.Add(-workingSetWindow - time.Second), bytes: 30 << 30}, // too old
		{at: now.Add(-40 * time.Second), bytes: 12 << 30},               // the spike
		{at: now.Add(-10 * time.Second), bytes: 6 << 30},
	}
	if got := workingSetPeak(backend.Stats{WorkingSetBytes: 5 << 30}, fast, now); got != 12<<30 {
		t.Fatalf("peak = %d GB, want the 12 GB spike between samples", got>>30)
	}
	if got := workingSetPeak(backend.Stats{WorkingSetBytes: 14 << 30}, fast, now); got != 14<<30 {
		t.Fatalf("peak = %d GB, want the sample's own 14 GB", got>>30)
	}
	if got := pruneWorkingSets(append([]workingSetSample(nil), fast...), now); len(got) != 2 || got[0].bytes != 12<<30 {
		t.Fatalf("pruned = %+v, want the two inside the window", got)
	}
	if got := workingSetPeak(backend.Stats{}, nil, now); got != 0 {
		t.Fatalf("nothing known gave %d, want 0", got)
	}
}
