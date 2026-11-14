package scheduler

import (
	"cmp"
	"fmt"
	"slices"
	"time"

	"github.com/eyupio/zoomies/internal/naming"
	"github.com/eyupio/zoomies/internal/store"
)

// Oldest-first admission (scheduler.oldest_first).
//
// Without it a pass hands room to pools in the order they were last served,
// and a pool whose runner does not fit anywhere is simply skipped. On a fleet
// whose hosts are full that is starvation for the larger runners: every time a
// job finishes and frees a little memory, the next pass gives it to whichever
// younger request fits in it (a 2 GB light runner, a 16 GB image build),
// and the 24 GB runner the oldest job is waiting for never finds the room
// whole. On 13 Nov 2026 a screenshot-chain root job waited 7.6-10.3 minutes
// that way while 18-56 jobs queued after it started on the same hosts.
//
// With it, each pass first lines up every pool's oldest unserved job, oldest
// first, across all pools, and holds capacity for it:
//
//   - a job whose runner fits somewhere has that room held for it, so a younger
//     request served earlier in the same pass cannot take it first;
//   - a job whose runner fits nowhere has its whole size held on the host where
//     it will fit soonest (the smallest shortfall), at most one such hold per
//     machine. The room there only grows as work finishes, so the job starts
//     the pass the host has room for it.
//
// A younger request may still start anywhere, held hosts included, in what is
// left after every older job's hold: backfill that cannot delay an older job.
// Nothing here changes a pool's size, a host's capacity or reserve, or which
// hosts a pool may use; it changes only the order the room already there is
// handed out in. Priority still comes first: a hold binds only pools of its own
// priority or lower.

const (
	// OldestFirstOn holds room for the oldest waiting jobs, as above.
	OldestFirstOn = "on"
	// OldestFirstOff hands room out as before the setting existed: least
	// recently served pool first, a pool that does not fit skipped.
	OldestFirstOff = "off"
)

// OldestFirstModes is every value scheduler.oldest_first takes.
var OldestFirstModes = []string{OldestFirstOn, OldestFirstOff}

func (p Policy) oldestFirst() bool { return p.OldestFirst == OldestFirstOn }

// Hold is capacity a pass kept for the oldest job waiting on a pool, on one
// host, so that younger work cannot take it first.
type Hold struct {
	JobID    string    `json:"job_id"`
	JobName  string    `json:"job_name"`
	Repo     string    `json:"repo"`
	QueuedAt time.Time `json:"queued_at"`
	PoolID   string    `json:"pool_id"`
	PoolName string    `json:"pool_name"`
	HostID   string    `json:"host_id"`
	HostName string    `json:"host_name"`
	// Fits says the room was there this pass and is only kept from younger
	// work until the job's runner is placed; false is a reservation for a job
	// that fits nowhere yet, on the host where it will fit soonest.
	Fits bool `json:"fits"`
	// CPUs and MemoryMB are the runner held for, at its size. BookedMB and
	// ChargeMB are what it is held against the booking and the measurement.
	CPUs     float64 `json:"cpus"`
	MemoryMB int64   `json:"memory_mb"`
	BookedMB int64   `json:"booked_mb"`
	ChargeMB int64   `json:"charge_mb"`

	priority int
}

// Phrase is the hold as the waiting job's timeline and the pool's reason say
// it: "capacity on ci-nl-02-trusted reserved for older job job_x (visual-build
// on pool zoomies-trusted-nodocker, queued 5m ago)".
func (h *Hold) Phrase(now time.Time) string {
	return fmt.Sprintf("capacity on %s reserved for older job %s (%s on pool %s, queued %s ago)",
		naming.ForSentence(h.HostName), h.JobID, naming.ForSentence(h.JobName), h.PoolName, formatDuration(now.Sub(h.QueuedAt)))
}

// olderThan orders jobs by when GitHub queued them, the ID breaking a tie, so
// the line is the same on every pass.
func olderThan(aAt time.Time, aID string, bAt time.Time, bID string) bool {
	if c := aAt.Compare(bAt); c != 0 {
		return c < 0
	}
	return aID < bID
}

// oldestUnserved is the oldest of a pool's eligible queued jobs that no runner
// is idle, starting or created for this pass: the runners there take the
// oldest jobs first, as GitHub hands them out. Nil when every eligible job has
// a runner on its way, or the pool scales for its minimum alone.
func oldestUnserved(pp *PoolPlan) *store.Job {
	i := pp.supply + creates(pp.Actions)
	if i < 0 || i >= len(pp.waiting) {
		return nil
	}
	return pp.waiting[i]
}

// sortWaiting puts a pool's eligible jobs in the order its runners take them.
func sortWaiting(jobs []*store.Job) {
	slices.SortStableFunc(jobs, func(a, b *store.Job) int {
		if c := a.QueuedAt.Compare(b.QueuedAt); c != 0 {
			return c
		}
		return cmp.Compare(a.ID, b.ID)
	})
}

// reserveOldest lines up every pool's oldest unserved job and holds room for
// each, oldest first, as the package comment above says.
func (t *tick) reserveOldest(pools []*store.Pool, byID map[string]*PoolPlan, runners map[string][]*store.Runner, demand map[string][]*store.Job) {
	type candidate struct {
		p   *store.Pool
		job *store.Job
	}
	var line []candidate
	for _, p := range pools {
		pp := byID[p.ID]
		if !p.Enabled || pp.Desired <= pp.Current+creates(pp.Actions) || pp.Failing != "" || pp.Held != "" {
			continue
		}
		if j := oldestUnserved(pp); j != nil {
			line = append(line, candidate{p: p, job: j})
		}
	}
	slices.SortStableFunc(line, func(a, b candidate) int {
		if olderThan(a.job.QueuedAt, a.job.ID, b.job.QueuedAt, b.job.ID) {
			return -1
		}
		if olderThan(b.job.QueuedAt, b.job.ID, a.job.QueuedAt, a.job.ID) {
			return 1
		}
		return cmp.Compare(a.p.ID, b.p.ID)
	})
	// One reservation for a job that fits nowhere per machine: it closes the
	// machine to younger work that does not fit beside it, and a second would
	// only close it for longer.
	reserved := map[string]bool{}
	for _, c := range line {
		applied := t.applyHolds(c.p, c.job)
		var hold *Hold
		if h := t.hosts.pick(c.p, nil); h != nil {
			hold = t.newHold(c.p, c.job, h, true, runners[c.p.ID], demand[c.p.ID])
		} else if h := t.hosts.soonest(c.p, reserved); h != nil {
			hold = t.newHold(c.p, c.job, h, false, runners[c.p.ID], demand[c.p.ID])
			reserved[h.ID] = true
			for _, id := range t.hosts.siblings[h.ID] {
				reserved[id] = true
			}
		}
		t.releaseHolds(applied)
		if hold != nil {
			t.holds = append(t.holds, hold)
		}
	}
}

// newHold is the room a runner of p for job would take on h: its size (raised
// for the jobs waiting, where history raise would raise it), what it books and
// what its start is charged.
func (t *tick) newHold(p *store.Pool, job *store.Job, h *store.Host, fits bool, runners []*store.Runner, queued []*store.Job) *Hold {
	hs := t.hosts
	res := Reserve(p, h)
	typed := res.MemoryMB
	if raise := t.raiseNeed(p, runners, queued); raise > typed {
		typed = raise
	}
	book := typed
	if hs.byHistory[h.ID] {
		book = hs.book.start(p, typed, nil)
	}
	return &Hold{
		JobID: job.ID, JobName: job.JobName, Repo: job.Repo, QueuedAt: job.QueuedAt,
		PoolID: p.ID, PoolName: p.Name, HostID: h.ID, HostName: hostName(h), Fits: fits,
		CPUs: res.CPUs, MemoryMB: typed, BookedMB: book, ChargeMB: startCharge(typed, 0, hs.startChargePercent),
		priority: p.Priority,
	}
}

// holdsAgainst is every hold a runner of p must leave alone: those for jobs
// older than p's own oldest unserved job (every hold, for a pool scaling for
// its minimum alone), at p's priority or above.
func (t *tick) holdsAgainst(p *store.Pool, mine *store.Job) []*Hold {
	var out []*Hold
	for _, h := range t.holds {
		if h.PoolID == p.ID || h.priority < p.Priority {
			continue
		}
		if mine != nil && !olderThan(h.QueuedAt, h.JobID, mine.QueuedAt, mine.ID) {
			continue
		}
		out = append(out, h)
	}
	return out
}

// applyHolds takes every hold p must leave alone out of the ledger, and
// returns them for releaseHolds to put back once p has been placed.
func (t *tick) applyHolds(p *store.Pool, mine *store.Job) []*Hold {
	holds := t.holdsAgainst(p, mine)
	for _, h := range holds {
		t.hosts.debitHold(h, 1)
	}
	return holds
}

func (t *tick) releaseHolds(holds []*Hold) {
	for _, h := range holds {
		t.hosts.debitHold(h, -1)
	}
}

// dropHold forgets p's hold once p has been given a runner: the room it kept
// has gone to the job it was kept for. The pool's next job lines up again on
// the next pass.
func (t *tick) dropHold(p *store.Pool) {
	t.holds = slices.DeleteFunc(t.holds, func(h *Hold) bool { return h.PoolID == p.ID })
}

// holdOf is p's own hold this pass, or nil.
func (t *tick) holdOf(p *store.Pool) *Hold {
	for _, h := range t.holds {
		if h.PoolID == p.ID {
			return h
		}
	}
	return nil
}

// debitHold takes a hold from the ledger (sign 1) or puts it back (sign -1),
// exactly as placing its runner would take it: a slot and its CPU on its host,
// its booking and start charge from the host and every agent sharing its
// machine. A hold for a job that fits nowhere leaves the room there negative,
// which is the point: nothing younger fits beside it.
func (hs *hostSet) debitHold(h *Hold, sign int64) {
	id := h.HostID
	hs.free[id] -= int(sign)
	l := hs.left[id]
	l.CPUs -= float64(sign) * h.CPUs
	hs.left[id] = l
	hs.promisedMemory[id] -= sign * h.MemoryMB
	for _, m := range append([]string{id}, hs.siblings[id]...) {
		if m == id || hs.byHistory[m] {
			hs.bookedMemory[m] -= sign * h.BookedMB
		}
		if mm, ok := hs.measuredMemory[m]; ok {
			hs.measuredMemory[m] = mm - sign*h.ChargeMB
		}
		hs.syncMemory(m)
	}
}

// soonest is the host a runner of p, which fits nowhere now, will fit on
// first: among the hosts that could ever run it and carry no other
// reservation, the one with the smallest shortfall, as a share of the runner,
// in memory and CPU, with a host out of slots or holding starts for CPU
// pressure counted a little further away. Nil where no host could ever run
// it, which the pool's own reason already says.
func (hs *hostSet) soonest(p *store.Pool, reserved map[string]bool) *store.Host {
	var best *store.Host
	bestScore := 0.0
	for _, h := range hs.hosts {
		if reserved[h.ID] || !HostCanRun(h, p, hs.now) {
			continue
		}
		need, left, a := Reserve(p, h), hs.leftFor(h, p), hs.alloc[h.ID]
		score := 0.0
		if a.MemoryKnown && need.MemoryMB > 0 && left.MemoryMB < need.MemoryMB {
			score += float64(need.MemoryMB-left.MemoryMB) / float64(need.MemoryMB)
		}
		if a.CPUsKnown && need.CPUs > 0 && left.CPUs < need.CPUs {
			score += (need.CPUs - left.CPUs) / need.CPUs
		}
		if hs.free[h.ID] <= 0 {
			score += 0.25
		}
		if hs.cpuPressureHolds(h) {
			score += 0.25
		}
		if best == nil || score < bestScore ||
			(score == bestScore && HostPriority(h).Score > HostPriority(best).Score) {
			best, bestScore = h, score
		}
	}
	return best
}

// heldBy is, for a pool whose runner did not fit once the holds were taken
// out, the hold that kept it out: one on the host it would have been placed on
// without them (or on an agent sharing that machine), or the oldest applied.
// Nil where it would not have been placed even without them.
func (t *tick) heldBy(p *store.Pool, avoid map[string]bool, applied []*Hold) *Hold {
	if len(applied) == 0 {
		return nil
	}
	h := t.hosts.pick(p, avoid)
	if h == nil {
		return nil
	}
	machine := append([]string{h.ID}, t.hosts.siblings[h.ID]...)
	for _, hold := range applied {
		if slices.Contains(machine, hold.HostID) {
			return hold
		}
	}
	return applied[0]
}
