package scheduler

import (
	"math"
	"slices"

	"github.com/eyupio/zoomies/internal/store"
)

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
func (b *booker) runner(p *store.Pool, h *store.Host, r *store.Runner) (booked, charge int64, basis string) {
	charge = RunnerCharge(p, h, r).MemoryMB
	if b == nil || charge <= 0 || r == nil || r.LentMemoryMB > 0 {
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
			if basis != basisTyped {
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
