package controller

import (
	"time"

	"github.com/eyupio/zoomies/internal/scheduler"
)

// startChargeTTL is how long a runner's start charge is remembered. A runner
// is pending only until a host sample contains it, which is a heartbeat or
// two once its container runs; one still provisioning past this (an image
// pull that outlasts it) is charged its whole size from then on, which errs
// the safe way.
const startChargeTTL = 30 * time.Minute

type recordedCharge struct {
	charge scheduler.Reservation
	at     time.Time
}

// recordStartCharge remembers what the scheduler charged the runner it just
// created against its host's measured free memory and CPU, so the passes until
// a host sample contains it charge it the same (Snapshot.StartCharges).
func (c *Controller) recordStartCharge(runnerID string, charge scheduler.Reservation, now time.Time) {
	if runnerID == "" {
		return
	}
	c.startChargesMu.Lock()
	defer c.startChargesMu.Unlock()
	if c.startCharges == nil {
		c.startCharges = map[string]recordedCharge{}
	}
	c.startCharges[runnerID] = recordedCharge{charge: charge, at: now}
}

// startChargesAt is the start charges a pass places against: never nil, so
// the scheduler charges a pending runner it has no record of (one created
// before this controller started) its whole size. Records older than
// startChargeTTL are dropped.
func (c *Controller) startChargesAt(now time.Time) map[string]scheduler.Reservation {
	c.startChargesMu.Lock()
	defer c.startChargesMu.Unlock()
	out := make(map[string]scheduler.Reservation, len(c.startCharges))
	for id, rc := range c.startCharges {
		if now.Sub(rc.at) > startChargeTTL {
			delete(c.startCharges, id)
			continue
		}
		out[id] = rc.charge
	}
	return out
}
