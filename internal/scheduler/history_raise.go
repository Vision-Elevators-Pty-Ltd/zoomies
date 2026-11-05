package scheduler

import (
	"fmt"

	"github.com/eyupio/zoomies/internal/store"
)

// A pool with a typed memory size gives every runner that size, and a job that
// has outgrown it is killed at it on every run until somebody edits the pool.
// Finding 70 (5 Nov) was the merge group's typecheck: typed at 24 GB, its p90
// rose from 17.3 to 23.4 GB in five days and runs were killed at exactly 24 GB.
// scheduler.history_raise lets such a runner hold what its job's own history
// says it needs, in the two ways the fleet has of giving a runner memory:
//
//   - at creation, where a job that needs more is waiting on the pool and a
//     host has room for the larger size (raiseAtCreate). This is the only way
//     for a docker-in-docker pair held to one shared limit: the agent sets that
//     limit on the pair's slice once, when the pair is created, and the memory
//     valve raises a container's own limit, which the slice would still cap;
//   - once GitHub has said which job a runner took, by giving it a memory-valve
//     rule whose ceiling is that job's need (HistoryGrantFor), for every runner
//     with limits of its own, whatever its pool's own valve setting is.
//
// Both are bounded the same way: never more than HistoryRaiseMaxFactor times
// the typed size, and never past what the host can hold (HistoryRaiseCap). The
// figure is historyBooking's, the one the overcommitted ledger books by: the
// ninetieth percentile of the job's recent peaks plus the booking headroom,
// from at least BookingMinSamples runs, so a job with a short history is never
// raised.

// The modes of scheduler.history_raise.
const (
	// HistoryRaiseOn raises a typed runner for its job's history.
	HistoryRaiseOn = "on"
	// HistoryRaiseOff keeps every runner at its pool's typed size, as the fleet
	// did before the setting existed.
	HistoryRaiseOff = "off"
)

// HistoryRaiseModes is every value scheduler.history_raise takes.
var HistoryRaiseModes = []string{HistoryRaiseOn, HistoryRaiseOff}

// HistoryRaiseMaxFactor is the most a runner may be raised to for its job's
// history, as a multiple of its pool's typed memory. A job whose history says
// more than twice what the operator typed is not one a raise should follow
// quietly: its history is more likely an outlier (a cold cache, a run the
// kernel killed and counted half as much again) than a size, and a pool that
// really needs more should say so.
const HistoryRaiseMaxFactor = 2

// historyRaise reports whether the policy raises typed runners for history.
func (p Policy) historyRaise() bool { return p.HistoryRaise == HistoryRaiseOn }

// raisesTyped reports whether a runner of p is one history_raise is for: a
// pool with a typed memory size, on a container backend, which is where a
// limit is set and can be given more of.
func raisesTyped(p *store.Pool) bool {
	return p != nil && p.Resources.MemoryMB > 0 && (p.Backend == store.BackendDocker || p.Backend == store.BackendPodman)
}

// raiseAtCreate reports whether a runner of p on h may be created above its
// typed size: its typed figure is the whole runner's limit, which is a pool
// with no daemon beside the runner or a pair held to one limit between them.
// A pair that gives each container the typed figure whole is charged twice
// it, and creating it larger would give both containers the raise; such a
// runner is raised by the valve once its job is known instead.
func raiseAtCreate(p *store.Pool, h *store.Host) bool {
	return raisesTyped(p) && p.TypedPairFactor(h) == 1
}

// raiseByValve reports whether a runner of p on h may be lent memory for its
// job's history. A shared-limit pair may not: the valve raises a container's
// own limit and the slice over the pair, set once at creation, still caps the
// two together, so the loan could never be used and would be charged all the
// same. That is the reason the API refuses the valve on such a pool.
func raiseByValve(p *store.Pool, h *store.Host) bool {
	return raisesTyped(p) && !p.SharesPairLimit(h)
}

// HistoryRaiseCap is the most one container of a runner of p on h may be
// raised to for its job's history: HistoryRaiseMaxFactor times the pool's
// typed memory, lowered to what the host can hold. That is its memory less the
// floor the valve keeps free (the reserve and a twentieth of the machine), its
// allocatable memory, the memory of the daemon where that is a smaller machine,
// and the ceiling the host's owner set for one runner, which has the last word
// here as it does for every loan. Zero for a pool with no typed memory.
func HistoryRaiseCap(p *store.Pool, h *store.Host) int64 {
	if p == nil || p.Resources.MemoryMB <= 0 {
		return 0
	}
	ceiling := p.Resources.MemoryMB * HistoryRaiseMaxFactor
	if h == nil {
		return ceiling
	}
	if h.MemoryMB > 0 {
		ceiling = min(ceiling, h.MemoryMB-MemoryPoolFloor(h))
	}
	if a := h.Allocatable(); a.MemoryKnown {
		ceiling = min(ceiling, a.MemoryMB)
	}
	if info, ok := h.BackendInfo.Find(p.Backend); ok && info.MemoryMB > 0 {
		ceiling = min(ceiling, info.MemoryMB)
	}
	if capMB := h.RunnerProfile.Standard.BurstMaxMemoryMB; capMB > 0 {
		ceiling = min(ceiling, capMB)
	}
	return max(ceiling, 0)
}

// HistoryGrant is the memory-valve rule a runner is given because the job it is
// running is known to need more than it was created with.
type HistoryGrant struct {
	// NeedMB is what the job's history says it needs (historyBooking).
	NeedMB int64 `json:"need_mb"`
	// TargetMB is what the runner's own container may grow to: the need,
	// bounded by HistoryRaiseCap.
	TargetMB int64 `json:"target_mb"`
	// GuaranteeMB is what the runner's containers were created with together,
	// and CeilingMB the most they may hold together: the guarantee, and the
	// difference between the target and what one container was given.
	GuaranteeMB int64 `json:"guarantee_mb"`
	CeilingMB   int64 `json:"ceiling_mb"`
}

// ExtraMB is what the grant may lend on top of the runner's guarantee.
func (g HistoryGrant) ExtraMB() int64 { return max(g.CeilingMB-g.GuaranteeMB, 0) }

// Reason says the grant the way the Runners page and the valve's reasons do.
func (g HistoryGrant) Reason() string {
	return fmt.Sprintf("raised for job history (need %d MB): may hold up to %d MB", g.NeedMB, g.CeilingMB)
}

// HistoryGrantFor is the grant a live runner r of p on h is given for a job
// whose history says it needs needMB, and false where it is given none: the
// pool is not one history_raise is for, the runner was created with enough,
// or the bound leaves nothing to raise.
//
// The need is what one container may grow to, compared with what one was
// given (the row's figure, which for a pair that gives each container the
// typed size whole is each container's). The job's peak is measured over the
// whole runner, so for such a pair it is an upper bound on either container,
// and the ceiling, which the valve holds both containers to together, is the
// guarantee plus the difference. A grant never lowers anything: a raised
// limit only ever goes up while the runner lives.
func HistoryGrantFor(p *store.Pool, h *store.Host, r *store.Runner, needMB int64) (HistoryGrant, bool) {
	if needMB <= 0 || r == nil || r.AllocatedMemoryMB <= 0 || !raiseByValve(p, h) {
		return HistoryGrant{}, false
	}
	guarantee, ok := LaunchedMemoryMB(p, h, r)
	if !ok || guarantee <= 0 {
		return HistoryGrant{}, false
	}
	target := min(needMB, HistoryRaiseCap(p, h))
	if target <= r.AllocatedMemoryMB {
		return HistoryGrant{}, false
	}
	ceiling := guarantee + target - r.AllocatedMemoryMB
	if h != nil && h.MemoryMB > 0 {
		ceiling = min(ceiling, h.MemoryMB)
	}
	if ceiling <= guarantee {
		return HistoryGrant{}, false
	}
	return HistoryGrant{NeedMB: needMB, TargetMB: target, GuaranteeMB: guarantee, CeilingMB: ceiling}, true
}

// runnerNeeds is, for every live runner whose job GitHub has named and whose
// job has a history to book from, what that history says the job needs, by
// runner ID: the figure HistoryGrantFor is given. Only runners whose need is
// above what they were created with are kept, because those are the only ones
// anything is done for. Nil with history_raise off.
func runnerNeeds(s Snapshot) map[string]int64 {
	if !s.Policy.historyRaise() || s.Booking == nil || len(s.Booking.RunnerJobs) == 0 {
		return nil
	}
	var out map[string]int64
	for _, p := range s.Pools {
		if !raisesTyped(p) {
			continue
		}
		for _, r := range s.Runners[p.ID] {
			if r == nil || !r.State.Live() || r.AllocatedMemoryMB <= 0 {
				continue
			}
			key, ok := s.Booking.RunnerJobs[r.ID]
			if !ok {
				continue
			}
			key.PoolID = p.ID
			need, known := historyBooking(s.JobHistory[key], s.Policy.MemoryBookingHeadroomPercent)
			if !known || need <= r.AllocatedMemoryMB {
				continue
			}
			if out == nil {
				out = map[string]int64{}
			}
			out[r.ID] = need
		}
	}
	return out
}

// raiseNeed is the most any job waiting on p is known to need, from the history
// of its runs on p (historyBooking), where that is above p's typed memory: what
// a runner created for p's queue now is raised to, where a host has room. Zero
// where nothing waiting needs more, where too little is known, or where the
// pool is not one history_raise is for.
//
// It is the largest, for the reason poolNeed's is: GitHub, not Zoomies,
// decides which waiting job an idle runner takes.
func (t *tick) raiseNeed(p *store.Pool, queued []*store.Job) int64 {
	if !t.policy.historyRaise() || !raisesTyped(p) {
		return 0
	}
	var need int64
	for _, j := range queued {
		if j == nil {
			continue
		}
		key := store.JobUsageKey{Repo: j.Repo, Workflow: j.Workflow, JobName: j.JobName, PoolID: p.ID}
		if mb, known := historyBooking(t.history[key], t.policy.MemoryBookingHeadroomPercent); known && mb > p.Resources.MemoryMB {
			need = max(need, mb)
		}
	}
	return need
}

// raiseTo is the memory a runner of p placed on h now is created with while
// the pass is placing for a raise (hs.raise): the need, bounded by
// HistoryRaiseCap, or zero where h cannot raise it above its typed size.
func (hs *hostSet) raiseTo(h *store.Host, p *store.Pool) int64 {
	if hs.raise <= 0 || !raiseAtCreate(p, h) {
		return 0
	}
	target := min(hs.raise, HistoryRaiseCap(p, h))
	if target <= Reserve(p, h).MemoryMB {
		return 0
	}
	return target
}

// raiseFits reports whether h has room for a runner of p at its raised size:
// the same room the pool's own size is asked for (memoryLeftAt: the booking,
// and the measured free memory less its start charge), asked of the larger
// figure, and within the host's allocatable memory.
func (hs *hostSet) raiseFits(h *store.Host, p *store.Pool) bool {
	target := hs.raiseTo(h, p)
	if target <= 0 {
		return false
	}
	if a := hs.alloc[h.ID]; a.MemoryKnown && a.MemoryMB < target {
		return false
	}
	return hs.memoryLeftAt(h, p, target) >= target
}

// raisedSize is the size a runner of p raised to targetMB on h is created at:
// the pool's own figures, with its memory raised. size is what the pass had
// already decided (a CPU raised by history sizing), kept where it says
// something.
func raisedSize(p *store.Pool, h *store.Host, res Reservation, size *store.Resources, targetMB int64) *store.Resources {
	grant := p.Resources
	grant.MinCPUs, grant.MinMemoryMB = 0, 0
	grant.CPUs = res.CPUs / fieldFactor(p, h, p.Resources.CPUs > 0)
	if size != nil {
		grant.CPUs = size.CPUs
	}
	grant.MemoryMB = targetMB
	return &grant
}
