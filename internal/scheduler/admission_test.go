package scheduler

import (
	"strings"
	"testing"
	"time"

	"github.com/eyupio/zoomies/internal/store"
)

// The fleet the change was measured on (6 Nov 2026): ci-nl-02 is one 62 GB
// machine with two agents booked at 300%, a ceiling of 3 x 54.4 = 163.3 GB.
// It refused 138 starts that day with 140-163 GB of the ceiling booked while
// 9-25 GB of its memory was in use, about 45 GB measured free.

func imageBuildPool() *store.Pool {
	p := limited("zoomies-image-build", 8, 16*gb)
	p.MaxRunners = 40
	return p
}

// usingMB is busyOn with the runner last measured using mb.
func usingMB(id string, p *store.Pool, h *store.Host, mb int64) *store.Runner {
	r := busyOn(id, p, h)
	r.MemoryBytes = mb << 20
	return r
}

// nl02 is ci-nl-02 as it stood at a refusal: two agents, ten image-build
// runners between them booked at their whole 16 GB (160 GB of the 163.3 GB
// ceiling) and each using 1.2 GB, with 45 GB measured free.
func nl02(admitByMeasure bool) (*hostSet, *store.Pool, *store.Host, *store.Host) {
	p := imageBuildPool()
	pilot := nlAgent("ci-nl-02-pilot", "ci-nl-02", 45*gb)
	trusted := nlAgent("ci-nl-02-trusted", "ci-nl-02", 45*gb)
	var runners []*store.Runner
	for i := range 10 {
		h := pilot
		if i%2 == 1 {
			h = trusted
		}
		runners = append(runners, usingMB("run_"+string(rune('a'+i)), p, h, 1229))
	}
	hs := newHostSetFrom([]*store.Host{pilot, trusted}, []*store.Pool{p}, map[string][]*store.Runner{p.ID: runners}, now,
		ledger{overcommitPercent: 300, cpuOvercommitPercent: 400, startChargePercent: 30, machineLabel: "host", admitByMeasure: admitByMeasure})
	return hs, p, pilot, trusted
}

// The worked example: booked, the machine refuses every image build because
// 160 GB of its 163.3 GB is booked. Measured, it takes what its 45 GB free
// memory admits past the reserve and the growth guard, and no more.
func TestMeasuredAdmissionStartsWhatTheMachineHasFree(t *testing.T) {
	booked, p, _, _ := nl02(false)
	if got := booked.place(p, 20); len(got) != 0 {
		t.Fatalf("booked admission placed %d image builds with 160 of 163.3 GB booked; vision.8 refused every one", len(got))
	}
	measured, p, pilot, trusted := nl02(true)
	// 45 GB free, less the 6 GB reserve and the growth guard: a runner booked
	// at 16 GB and using 1.2 GB may grow by 14.8 GB. The first start is booked
	// at 16 GB, which raises the guard by 1.2 GB, and each start is charged
	// 30% of 16 GB = 4.8 GB: (45 - 6 - 14.8 - 1.2) / 4.8 = 4.79, so 4 starts.
	growth := 16*gb - 1229
	if g := measured.growth[pilot.ID]; g != growth || measured.growth[trusted.ID] != growth {
		t.Fatalf("the growth guard is %d and %d MB, want the machine's %d on both agents", g, measured.growth[trusted.ID], growth)
	}
	free := 45*gb - pilot.MemoryReserve() - growth
	want := int((free - (16*gb - growth)) / startCharge(16*gb, 0, 30))
	got := measured.placeAvoiding(p, 20, nil)
	if want != 4 || len(got) != want {
		t.Fatalf("measured admission placed %d image builds, want %d (the example says 4)", len(got), want)
	}
	for _, pl := range got {
		if pl.size != nil {
			t.Fatalf("a runner was created at %+v; its limit must stay the pool's 16 GB", *pl.size)
		}
	}
	// The booking was still taken from, for the Hosts page and the score.
	if left := measured.bookedMemory[pilot.ID]; left != BookingMemoryMB(pilot, 300)-160*gb-int64(want)*16*gb {
		t.Fatalf("the booking has %d MB left after %d starts", left, want)
	}
	if hb := measured.bookings[pilot.ID]; hb == nil || hb.Admission != AdmissionMeasured || hb.GrowthReserveMB != growth {
		t.Fatalf("the host's booking view is %+v", hb)
	}
}

// The measured check is unchanged under measured admission: nothing starts
// where the measurement is short of the reserve and one start charge, however
// much room the booking has, and at or below the reserve the pressure hold
// refuses the host outright.
func TestMeasuredAdmissionKeepsTheMeasuredCheckAndTheReserve(t *testing.T) {
	p := imageBuildPool()
	h := nlAgent("ci-nl-02-pilot", "", 0)
	runners := map[string][]*store.Runner{p.ID: {usingMB("run_a", p, h, 2*gb)}}
	at := func(freeMB int64) *hostSet {
		h.Usage.MemoryAvailableMB = &freeMB
		return newHostSetFrom([]*store.Host{h}, []*store.Pool{p}, runners, now,
			ledger{overcommitPercent: 300, startChargePercent: 30, admitByMeasure: true})
	}
	short := h.MemoryReserve() + startCharge(16*gb, 0, 30) - 1
	hs := at(short)
	if got := hs.place(p, 20); len(got) != 0 {
		t.Fatalf("placed %d one MB short of the reserve and a start charge, with 147 GB of booking left", len(got))
	}
	if d := hs.shortOfMemory(h, p); !strings.Contains(d, "measured free") || !strings.Contains(d, "is charged") {
		t.Fatalf("short of memory says %q, want the measurement named", d)
	}
	if got := at(short+1).place(p, 20); len(got) != 1 {
		t.Fatalf("placed %d with exactly the reserve and one charge free, want 1", len(got))
	}
	if got := at(h.MemoryReserve()).place(p, 20); len(got) != 0 {
		t.Fatalf("placed %d on a host whose free memory is at its reserve", len(got))
	}
}

// Where the booking is full, a start needs the reserve, the growth guard and
// its charge measured free: one MB short refuses it, exactly enough takes one.
func TestMeasuredAdmissionPastAFullBookingHoldsTheGrowthGuard(t *testing.T) {
	p := imageBuildPool()
	h := nlAgent("ci-nl-02-pilot", "", 0)
	var rs []*store.Runner
	for i := range 10 { // 160 GB booked of the 163.3 GB ceiling
		rs = append(rs, usingMB("run_"+string(rune('a'+i)), p, h, 2*gb))
	}
	runners := map[string][]*store.Runner{p.ID: rs}
	at := func(freeMB int64) *hostSet {
		h.Usage.MemoryAvailableMB = &freeMB
		return newHostSetFrom([]*store.Host{h}, []*store.Pool{p}, runners, now,
			ledger{overcommitPercent: 300, cpuOvercommitPercent: 400, startChargePercent: 30, admitByMeasure: true})
	}
	// The runners there may grow by 14 GB each, but a start booked at its
	// whole 16 GB (no history) may grow by 16: the guard it must leave is 16 GB.
	short := h.MemoryReserve() + 16*gb + startCharge(16*gb, 0, 30) - 1
	hs := at(short)
	if hs.growth[h.ID] != 14*gb {
		t.Fatalf("growth guard %d MB, want 14 GB", hs.growth[h.ID])
	}
	if got := hs.place(p, 20); len(got) != 0 {
		t.Fatalf("placed %d one MB short of the reserve, the growth guard and a start charge", len(got))
	}
	if d := hs.shortOfMemory(h, p); !strings.Contains(d, "may still grow by") || !strings.Contains(d, "nor has its booking room") {
		t.Fatalf("short of memory says %q, want the growth guard and the booking named", d)
	}
	if got := at(short+1).place(p, 20); len(got) != 1 {
		t.Fatalf("placed %d with exactly the reserve, the guard and one charge free, want 1", len(got))
	}
}

// Measured admission never refuses what booked admission takes: on a quiet
// machine whose booking has room, a heavy idle runner's guard does not cost a
// start the measurement alone allowed before.
func TestMeasuredAdmissionNeverAdmitsLessThanBooked(t *testing.T) {
	p := browserPool()
	h := nlAgent("ci-nl-01-pr", "", 47*gb)
	idle := usingMB("run_idle", p, h, 200) // booked whole (no history): a 32 GB guard
	runners := map[string][]*store.Runner{p.ID: {idle}}
	count := func(measure bool) int {
		hs := newHostSetFrom([]*store.Host{h}, []*store.Pool{p}, runners, now,
			ledger{overcommitPercent: 300, cpuOvercommitPercent: 400, startChargePercent: 30, admitByMeasure: measure})
		return len(hs.place(p, 20))
	}
	booked, measured := count(false), count(true)
	if booked == 0 || measured != booked {
		t.Fatalf("booked admission placed %d and measured %d; measured must take at least what booked does", booked, measured)
	}
}

// The guard is the largest growth still to come, not the sum: runners rarely
// peak together, and summing every peak is the booking this replaces. A start
// booked for more than the guard raises it, on every agent of the machine.
func TestTheGrowthGuardIsTheHeaviestRunnerAndRisesWithAStart(t *testing.T) {
	p := browserPool()
	pr := nlAgent("ci-nl-01-pr", "ci-nl-01", 49*gb)
	tr := nlAgent("ci-nl-01-trusted", "ci-nl-01", 49*gb)
	visual, draft := usingMB("run_v", p, pr, 3*gb), usingMB("run_d", p, tr, 4*gb)
	history := map[store.JobUsageKey][]store.JobPeak{
		key("visual-shard (1/3)", p): peaksOf(20, 8*gb),
		key("draft-affected", p):     peaksOf(20, 22*gb),
	}
	in := &BookingInputs{
		PoolRuns:   map[string][]store.JobPeak{p.ID: peaksOf(100, 4*gb)},
		RunnerJobs: map[string]store.JobUsageKey{visual.ID: key("visual-shard (1/3)", p), draft.ID: key("draft-affected", p)},
	}
	b := newBooker(BookingHistory, 25, in, history, []*store.Pool{p}, nil)
	hs := newHostSetFrom([]*store.Host{pr, tr}, []*store.Pool{p}, map[string][]*store.Runner{p.ID: {visual, draft}}, now,
		ledger{overcommitPercent: 300, startChargePercent: 30, machineLabel: "host", book: b, admitByMeasure: true})
	// draft-affected books 22 GB x 1.25 = 27.5 GB and uses 4 GB: 23.5 GB to come.
	if want := int64(28160 - 4*gb); hs.growth[pr.ID] != want || hs.growth[tr.ID] != want {
		t.Fatalf("growth guard %d / %d MB, want draft-affected's %d on both agents", hs.growth[pr.ID], hs.growth[tr.ID], want)
	}
	before := hs.measuredMemory[pr.ID]
	// A start books the pool's 5 GB, below the guard: only its charge is taken.
	if got := hs.place(p, 1); len(got) != 1 {
		t.Fatal("no start on a machine with room")
	}
	if spent := before - hs.measuredMemory[tr.ID]; spent != startCharge(32*gb, 0, 30) {
		t.Fatalf("a start below the guard took %d MB from the sibling's measurement, want its charge", spent)
	}
	// A start booked above the guard raises it by the difference on both.
	hs.raiseGrowth(pr, 30*gb)
	if hs.growth[pr.ID] != 30*gb || hs.growth[tr.ID] != 30*gb {
		t.Fatalf("raising the guard left %d / %d MB", hs.growth[pr.ID], hs.growth[tr.ID])
	}
}

// The booking is a soft hint: between two machines with the same free memory,
// the one booked less takes the start, though either could.
func TestTheBookingOrdersHostsUnderMeasuredAdmission(t *testing.T) {
	p := imageBuildPool()
	light := nlAgent("ci-nl-01-trusted", "", 45*gb)
	heavy := nlAgent("ci-nl-02-trusted", "", 45*gb)
	var runners []*store.Runner
	for i := range 6 {
		runners = append(runners, usingMB("heavy_"+string(rune('a'+i)), p, heavy, 1229))
	}
	runners = append(runners, usingMB("light_a", p, light, 1229))
	hs := newHostSetFrom([]*store.Host{heavy, light}, []*store.Pool{p}, map[string][]*store.Runner{p.ID: runners}, now,
		ledger{overcommitPercent: 300, cpuOvercommitPercent: 400, startChargePercent: 30, admitByMeasure: true})
	if got := hs.place(p, 1); len(got) != 1 || got[0] != light.ID {
		t.Fatalf("the start went to %v, want the host booked less (%s)", got, light.ID)
	}
}

// Nothing changes at 100% overcommit: there is no booking ceiling to lift.
func TestMeasuredAdmissionChangesNothingWithoutOvercommit(t *testing.T) {
	p := imageBuildPool()
	h := nlAgent("ci-nl-02-pilot", "", 45*gb)
	runners := map[string][]*store.Runner{p.ID: {usingMB("run_a", p, h, 1229), usingMB("run_b", p, h, 1229)}}
	off := newHostSetFrom([]*store.Host{h}, []*store.Pool{p}, runners, now, ledger{overcommitPercent: 100, startChargePercent: 30})
	on := newHostSetFrom([]*store.Host{h}, []*store.Pool{p}, runners, now, ledger{overcommitPercent: 100, startChargePercent: 30, admitByMeasure: true})
	if on.byMeasure || on.growth != nil {
		t.Fatal("measured admission is on at 100% overcommit")
	}
	if a, b := len(off.place(p, 20)), len(on.place(p, 20)); a != b {
		t.Fatalf("placed %d booked and %d measured at 100%%", a, b)
	}
}

// The kill switch: scheduler.memory_admission=booked, through Decide, is the
// vision.8 plan; measured creates where the machine has memory free.
func TestMemoryAdmissionIsAKillSwitchThroughThePolicy(t *testing.T) {
	plan := func(mode string) Plan {
		p := imageBuildPool()
		p.Labels = store.StringSlice{"self-hosted", "zoomies-image-build"}
		p.HostSelector = store.StringMap{}
		pilot := nlAgent("ci-nl-02-pilot", "ci-nl-02", 45*gb)
		trusted := nlAgent("ci-nl-02-trusted", "ci-nl-02", 45*gb)
		trusted.Labels["pool"] = "trusted"
		var runners []*store.Runner
		var jobs []*store.Job
		for i := range 10 {
			h := pilot
			if i%2 == 1 {
				h = trusted
			}
			r := usingMB("run_"+string(rune('a'+i)), p, h, 1229)
			r.PoolID = p.ID
			runners = append(runners, r)
		}
		for i := range 6 {
			j := queued("job_"+string(rune('a'+i)), time.Minute, "self-hosted", "zoomies-image-build")
			jobs = append(jobs, j)
		}
		s := snap([]*store.Pool{p}, runners, jobs, []*store.Host{pilot, trusted})
		s.Policy.MemoryOvercommitPercent, s.Policy.CPUOvercommitPercent, s.Policy.StartChargePercent = 300, 400, 30
		s.Policy.MemoryBooking, s.Policy.MemoryBookingHeadroomPercent, s.Policy.MachineLabel = BookingHistory, 25, "host"
		s.Policy.MemoryAdmission = mode
		s.Booking = &BookingInputs{}
		return Decide(s)
	}
	if got := actionsOf(plan(AdmissionBooked).Actions, ActionCreate); len(got) != 0 {
		t.Fatalf("booked admission created %d runners on a machine booked 160 of 163.3 GB", len(got))
	}
	if got := actionsOf(plan("").Actions, ActionCreate); len(got) != 0 {
		t.Fatalf("an unset admission created %d runners; the zero value must be booked", len(got))
	}
	got := actionsOf(plan(AdmissionMeasured).Actions, ActionCreate)
	if len(got) == 0 {
		t.Fatal("measured admission created nothing on a machine with 45 GB free")
	}
	if len(got) > 4 {
		t.Fatalf("measured admission created %d runners; the measurement admits 4", len(got))
	}
}
