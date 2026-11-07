package scheduler

import (
	"math"
	"slices"

	"github.com/eyupio/zoomies/internal/store"
)

// ElasticMemoryFeature is agent.FeatureElasticMemory, the word an agent's host
// row carries when it can lend memory. It is written out because this package
// imports no agent code; a test holds the two equal.
const ElasticMemoryFeature = "elastic-memory"

// The modes of scheduler.memory_booking.
const (
	// BookingHistory books a runner in the overcommitted memory ledger at what
	// its job is known to use: the ninetieth percentile of the job's recent
	// peaks with a headroom on top, or, before its job is known, the larger of
	// its pool's and the waiting jobs' figure. Never above the runner's size,
	// and the size itself wherever too little is known.
	BookingHistory = "history"
	// BookingTyped books every runner at its whole size, as the fleet always did.
	BookingTyped = "typed"
)

// BookingModes is every value scheduler.memory_booking takes.
var BookingModes = []string{BookingHistory, BookingTyped}

// The modes of scheduler.memory_admission: what decides whether a host in the
// overcommitted ledger (memory overcommit above 100, a fresh sample) has
// memory for one more runner.
const (
	// AdmissionMeasured admits a start on the machine's measured free memory:
	// its sampled available memory, less the starts that sample cannot contain
	// yet, the reserve and the largest growth still to come of any one runner
	// on it (the growth guard, HostBooking.GrowthReserveMB), must cover the
	// start's charge. What the runners are booked for no longer refuses a
	// start: it orders the hosts that have room. A start the booked rule
	// below admits is admitted too, so measured admission never takes fewer
	// starts than booked admission did.
	//
	// The booking sums each runner's ninetieth-percentile peak, and runners
	// rarely peak together: on 6 November ci-nl-02 refused 138 starts with
	// 140-163 GB of its 163 GB ceiling booked while its 62 GB of memory had
	// 9-25 GB in use, at about half its CPU.
	AdmissionMeasured = "measured"
	// AdmissionBooked also holds every start to the booking ceiling: the
	// machine's allocatable memory times the overcommit, against what every
	// runner on it is booked for. The behaviour of 1.4.1-vision.8 and before.
	AdmissionBooked = "booked"
)

// AdmissionModes is every value scheduler.memory_admission takes.
var AdmissionModes = []string{AdmissionMeasured, AdmissionBooked}

// admitByMeasure reports whether the policy admits starts on the measured
// free memory alone (scheduler.memory_admission=measured). Anything else, the
// zero value included, keeps the booking ceiling.
func (p Policy) admitByMeasure() bool { return p.MemoryAdmission == AdmissionMeasured }

const (
	// BookingMinSamples is the fewest measured runs a booking is taken from.
	// Fewer, and the runner is booked at its whole size: one or two runs say
	// little about the next.
	BookingMinSamples = 5
	// PoolBookingRuns is how many of a pool's most recent measured runs its
	// booking is taken from.
	PoolBookingRuns = 100
	// DefaultBookingHeadroomPercent is scheduler.memory_booking_headroom_percent
	// unset: a fourth more than the ninetieth percentile.
	DefaultBookingHeadroomPercent = 25
	// DefaultMachineLabel is scheduler.machine_label unset: hosts carrying the
	// same value of this label are agents on one machine and share one memory
	// ceiling.
	DefaultMachineLabel = "host"
)

// BookingInputs is what the overcommitted memory ledger books runners from,
// read by the caller like the job history: the scheduler reads no database.
type BookingInputs struct {
	// PoolRuns is each pool's most recent measured runs, newest first, at most
	// PoolBookingRuns of them, keyed by pool ID.
	PoolRuns map[string][]store.JobPeak
	// RunnerJobs is the job GitHub says each runner is running, keyed by runner
	// ID. Its history is in Snapshot.JobHistory under the runner's pool.
	RunnerJobs map[string]store.JobUsageKey
}

// historyBooking is what the runs of one job, or of one pool, say a runner
// running it should be booked for: the ninetieth percentile of their peaks
// with headroomPercent more, rounded up to a size, and never below what the
// largest run the kernel killed for memory needed. A killed run counts as
// having needed half as much again as the limit it hit, as in Profile. ok is
// false when there are fewer than BookingMinSamples runs to go on.
func historyBooking(peaks []store.JobPeak, headroomPercent int) (int64, bool) {
	var mem []int64
	var killed int64
	for _, p := range peaks {
		m := p.MemoryMB
		if p.OOMKilled {
			m = int64(math.Ceil(float64(max(m, p.GrantedMemoryMB)) * ProfileOOMGrowth))
			killed = max(killed, m)
		}
		if m > 0 {
			mem = append(mem, m)
		}
	}
	if len(mem) < BookingMinSamples {
		return 0, false
	}
	slices.Sort(mem)
	need := max(mem[percentileIndex(len(mem))], killed)
	scaled := math.Ceil(float64(need) * float64(100+max(headroomPercent, 0)) / 100)
	return int64(math.Ceil(scaled/profileMemoryStep)) * profileMemoryStep, true
}

// The bases a runner's booking is taken from, as the Hosts page and a reason say them.
const (
	// basisTyped is the runner's whole size: booking is typed, too little is
	// known, or the runner has been lent memory.
	basisTyped = "typed"
	// basisJob is the history of the job the runner is running.
	basisJob = "job"
	// basisPool is the pool's history, or the jobs waiting on it: the runner
	// has no job yet.
	basisPool = "pool"
	// basisRaised is a runner whose job needs more than its size, which
	// scheduler.history_raise lets it grow into (HistoryGrantFor): booked at
	// what it may grow to, above its size.
	basisRaised = "raised"
)

// booker books runners by history. A nil booker books every runner at its
// whole size.
type booker struct {
	headroom   int
	jobs       map[store.JobUsageKey][]store.JobPeak
	runnerJobs map[string]store.JobUsageKey
	// unknown is what a runner of each pool is booked for while its job is not
	// known, by pool ID; a pool missing from it books such a runner whole.
	unknown map[string]int64
	// byJob is each job's booking, worked out once a pass: several runners
	// run the same job, and every live runner is booked on every pass.
	byJob map[store.JobUsageKey]jobBooking
	// raise is scheduler.history_raise on: a runner whose job needs more than
	// its size, and that the memory valve will let grow into it, is booked at
	// what it may grow to.
	raise bool
}

type jobBooking struct {
	mb    int64
	known bool
}

// job is the booking the history of one job on one pool gives, and whether it
// gives one.
func (b *booker) job(key store.JobUsageKey) (int64, bool) {
	if got, ok := b.byJob[key]; ok {
		return got.mb, got.known
	}
	mb, known := historyBooking(b.jobs[key], b.headroom)
	if b.byJob == nil {
		b.byJob = map[store.JobUsageKey]jobBooking{}
	}
	b.byJob[key] = jobBooking{mb, known}
	return mb, known
}

// newBooker works out each pool's booking for a runner whose job is not yet
// known: the larger of what the pool's own recent runs say and what the
// heaviest job now waiting on it is known to use. GitHub, not Zoomies, decides
// which waiting job an idle runner takes, so a runner that may yet take the
// heavy one is booked for it; the pool's own figure covers the jobs whose
// history is too short to say. A pool with too little history of its own books
// such a runner whole.
func newBooker(mode string, headroom int, in *BookingInputs, history map[store.JobUsageKey][]store.JobPeak, pools []*store.Pool, demand map[string][]*store.Job) *booker {
	if mode != BookingHistory || in == nil {
		return nil
	}
	b := &booker{headroom: headroom, jobs: history, runnerJobs: in.RunnerJobs, unknown: map[string]int64{}}
	for _, p := range pools {
		if p == nil {
			continue
		}
		mb, ok := historyBooking(in.PoolRuns[p.ID], headroom)
		if !ok {
			continue
		}
		for _, j := range demand[p.ID] {
			if j == nil {
				continue
			}
			if need, known := b.job(store.JobUsageKey{Repo: j.Repo, Workflow: j.Workflow, JobName: j.JobName, PoolID: p.ID}); known {
				mb = max(mb, need)
			}
		}
		b.unknown[p.ID] = mb
	}
	return b
}

// runner is what one live runner r of p on h is booked for in the
// overcommitted ledger, its whole charge (RunnerCharge) and the basis.
//
// It is the job's own history once GitHub has said which job the runner took,
// and the pool's figure (newBooker) before. It is never more than the charge,
// because a runner cannot use more than its limit, and never less than what
// the runner was last measured using, because a job heavier than its history
// is heavier now. A runner lent memory by the valve is booked whole: it was
// lent because it was using what it had. A job with too little history books
// its runner whole.
//
// The one exception to "never more than the charge" is a runner whose job is
// heavier than its size under scheduler.history_raise: the valve will let it
// grow to its job's need (HistoryGrantFor), so it is booked at that ceiling,
// lent memory or not, and the machine's one ceiling counts what it is about to
// take rather than what it was created with.
func (b *booker) runner(p *store.Pool, h *store.Host, r *store.Runner) (booked, charge int64, basis string) {
	charge = RunnerCharge(p, h, r).MemoryMB
	if b == nil || charge <= 0 || r == nil {
		return charge, charge, basisTyped
	}
	if b.raise && h != nil && h.Supports(ElasticMemoryFeature) {
		// Only where the agent can lend: elsewhere the grant is never carried
		// out, and booking it would hold memory for a raise that cannot come.
		if key, ok := b.runnerJobs[r.ID]; ok {
			key.PoolID = p.ID
			if need, known := raiseNeedMB(b.jobs[key], b.headroom); known {
				if g, ok := HistoryGrantFor(p, h, r, need); ok && g.CeilingMB > charge {
					return g.CeilingMB, charge, basisRaised
				}
			}
		}
	}
	if r.LentMemoryMB > 0 {
		return charge, charge, basisTyped
	}
	var need int64
	if key, ok := b.runnerJobs[r.ID]; ok {
		key.PoolID = p.ID
		mb, known := b.job(key)
		if !known {
			return charge, charge, basisTyped
		}
		need, basis = mb, basisJob
	} else {
		mb, known := b.unknown[p.ID]
		if !known {
			return charge, charge, basisTyped
		}
		need, basis = mb, basisPool
	}
	need = max(need, r.MemoryBytes>>20)
	if need >= charge {
		return charge, charge, basisTyped
	}
	return need, charge, basis
}

// start is what a runner of p placed now is booked for, at a size of sizeMB:
// its pool's figure for a runner whose job is not yet known, raised to what
// the jobs waiting need where the pass is placing for that (need, from
// scheduler.history_sizing=on), never above the size. The size itself where
// the pool's figure is not known.
func (b *booker) start(p *store.Pool, sizeMB int64, need *Requirement) int64 {
	if b == nil || sizeMB <= 0 {
		return sizeMB
	}
	mb, known := b.unknown[p.ID]
	if !known {
		return sizeMB
	}
	if need != nil {
		mb = max(mb, need.MemoryMB)
	}
	return min(mb, sizeMB)
}

// machineOf is the machine a host is an agent on: the value of its
// machineLabel label, or "" for a host that is a machine of its own. Two agents
// on one box (a pull-request agent and a trusted one, each under its own
// user) each report the whole machine, and each would otherwise be booked
// for all of it.
func machineOf(h *store.Host, label string) string {
	if label == "" || h == nil {
		return ""
	}
	return h.Labels[label]
}

// HostBooking is how the overcommitted memory ledger saw one host in a pass:
// what its runners are booked for against what they were typed, and the
// machine whose ceiling they count against.
type HostBooking struct {
	// Mode is scheduler.memory_booking.
	Mode string `json:"mode"`
	// BookedMemoryMB is what the live runners on this host are booked for, and
	// TypedMemoryMB their whole sizes, which is what the ledger booked before
	// scheduler.memory_booking=history. ByHistory is how many runners are booked
	// below their size.
	BookedMemoryMB int64 `json:"booked_memory_mb"`
	TypedMemoryMB  int64 `json:"typed_memory_mb"`
	ByHistory      int   `json:"by_history,omitempty"`
	// RaisedByHistory is how many runners are booked above their size, at
	// what scheduler.history_raise lets them grow to for their job.
	RaisedByHistory int `json:"raised_by_history,omitempty"`
	// Machine is the value of scheduler.machine_label the host shares with
	// other agents, empty for a host that is a machine of its own.
	// MachineBookedMemoryMB is what every agent on that machine is booked for,
	// and MachineCeilingMB what they may be booked for between them. For a
	// host alone both are its own.
	Machine               string   `json:"machine,omitempty"`
	MachineHosts          []string `json:"machine_hosts,omitempty"`
	MachineBookedMemoryMB int64    `json:"machine_booked_memory_mb"`
	MachineTypedMemoryMB  int64    `json:"machine_typed_memory_mb"`
	MachineCeilingMB      int64    `json:"machine_ceiling_mb,omitempty"`
	// Overcommitted says the ledger above is the one the pass placed by: memory
	// is booked past the machine and the host had a fresh sample. Otherwise the
	// host is booked its runners' whole sizes against its own allocatable memory.
	Overcommitted bool `json:"overcommitted"`
	// Admission is scheduler.memory_admission. Measured, on an overcommitted
	// host, admits a start on the measured free memory alone and reads the
	// booking only to order hosts; booked also holds it to the ceiling.
	Admission string `json:"admission,omitempty"`
	// GrowthReserveMB is the most any one live runner on this host (on its
	// machine, for an agent sharing one) may still grow by: what it is booked
	// for less what it was last measured using. Under measured admission it is
	// held back from the measured free memory with the reserve, so a start
	// never takes the memory the heaviest job already running is about to
	// need.
	GrowthReserveMB int64 `json:"growth_reserve_mb,omitempty"`
}

// bookingTotals adds up what the live runners on each host are booked for and
// typed at, by host ID, in one pass over the fleet.
func bookingTotals(hosts []*store.Host, pools []*store.Pool, runners map[string][]*store.Runner, b *booker) map[string]*HostBooking {
	out := make(map[string]*HostBooking, len(hosts))
	byID := make(map[string]*store.Host, len(hosts))
	for _, h := range hosts {
		if h == nil {
			continue
		}
		if _, dup := byID[h.ID]; !dup {
			byID[h.ID] = h
			out[h.ID] = &HostBooking{}
		}
	}
	for _, p := range pools {
		if p == nil {
			continue
		}
		for _, r := range runners[p.ID] {
			if r == nil || !r.State.Live() {
				continue
			}
			h, ok := byID[r.HostID]
			if !ok {
				continue
			}
			booked, charge, basis := b.runner(p, h, r)
			t := out[h.ID]
			t.BookedMemoryMB += booked
			t.TypedMemoryMB += charge
			// What it may still grow by: a runner booked for more than it is
			// using now can take that much more of the machine without a start.
			t.GrowthReserveMB = max(t.GrowthReserveMB, booked-r.MemoryBytes>>20)
			switch basis {
			case basisTyped:
			case basisRaised:
				t.RaisedByHistory++
			default:
				t.ByHistory++
			}
		}
	}
	return out
}

// machineGroups is the hosts that share a machine, by machine, each listed
// once in host order. A machine with one agent is not a group.
func machineGroups(hosts []*store.Host, label string) map[string][]*store.Host {
	if label == "" {
		return nil
	}
	var out map[string][]*store.Host
	seen := map[string]bool{}
	all := map[string][]*store.Host{}
	for _, h := range hosts {
		m := machineOf(h, label)
		if m == "" || seen[h.ID] {
			continue
		}
		seen[h.ID] = true
		all[m] = append(all[m], h)
	}
	for m, hs := range all {
		if len(hs) < 2 {
			continue
		}
		if out == nil {
			out = map[string][]*store.Host{}
		}
		out[m] = hs
	}
	return out
}

// pendingAgainst is the memory of the live runners on hs that a sample taken
// at sampledAt cannot yet contain: those still starting, and those created or
// started since. It is hostTotals.add's pending, asked against one sample for
// runners on several agents of one machine.
func pendingAgainst(hs []*store.Host, pools []*store.Pool, runners map[string][]*store.Runner, sample *store.Host) int64 {
	on := make(map[string]*store.Host, len(hs))
	for _, h := range hs {
		on[h.ID] = h
	}
	var out int64
	for _, p := range pools {
		if p == nil {
			continue
		}
		for _, r := range runners[p.ID] {
			if r == nil || !r.State.Live() {
				continue
			}
			h, ok := on[r.HostID]
			if !ok {
				continue
			}
			starting := r.State == store.RunnerProvisioning || r.State == store.RunnerRegistering
			if starting || r.CreatedAt.After(sample.Usage.SampledAt) ||
				(r.ContainerStartedAt != nil && r.ContainerStartedAt.After(sample.Usage.SampledAt)) {
				out += RunnerCharge(p, h, r).MemoryMB
			}
		}
	}
	return out
}
