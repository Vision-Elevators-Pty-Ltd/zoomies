package controller

import (
	"context"
	"fmt"
	"strings"

	"github.com/eyupio/zoomies/internal/agent"
	"github.com/eyupio/zoomies/internal/backend"
	"github.com/eyupio/zoomies/internal/scheduler"
	"github.com/eyupio/zoomies/internal/store"
)

// A runner that maps host devices (runner_settings.devices) is placed by the
// pool's devices and host_selector when it is created, but a create task can
// wait in its host's queue, and the pool or the host can change in between:
// the pool's devices cleared or changed, the host's kvm=ok label removed, or
// its agent swapped for one that cannot map them. The scheduler retires
// runners made under the old policy (scheduler.staleDevices); this is the same
// check at the last moment a create can still be stopped, as it is handed to
// the agent, so a create queued before the change is never delivered.

// gateDeviceCreates is tasks less every create of a device-mapping runner its
// pool or host no longer allows. Each refused create's lease is cleared and
// its runner failed with the reason, so the reap cleans it (and its
// registration) up like any runner that failed to start. Every other task
// passes untouched and unread: a pool that maps nothing costs nothing here.
func (c *Controller) gateDeviceCreates(ctx context.Context, hostID string, q *taskQueue, tasks []agent.Task) []agent.Task {
	out := tasks[:0:0]
	for _, t := range tasks {
		if t.Kind != agent.TaskCreateRunner || t.Spec == nil || len(t.Spec.Devices) == 0 {
			out = append(out, t)
			continue
		}
		why := c.deviceCreateRefusal(ctx, hostID, t.Spec)
		if why == "" {
			out = append(out, t)
			continue
		}
		q.complete(t.ID)
		c.log.Warn("did not deliver a runner create whose device policy changed while it was queued",
			"runner", t.RunnerID, "host", hostID, "reason", why)
		if err := c.failRunner(ctx, t.RunnerID, "not created: "+why, store.FaultConfig, true); err != nil {
			c.log.Warn("could not fail a runner whose create was withheld", "runner", t.RunnerID, "error", err)
		}
	}
	return out
}

// deviceCreateRefusal says why a create of a runner mapping spec.Devices may
// not go to hostID now, or "" where it may: the pool still maps exactly those
// devices and the host still takes it (scheduler.HostSelects: its labels, and
// an agent that maps devices). A pool or host that cannot be read refuses:
// the safe answer for a runner that would be given a host device.
func (c *Controller) deviceCreateRefusal(ctx context.Context, hostID string, spec *backend.Spec) string {
	pool, err := c.st.GetPool(ctx, spec.PoolID)
	if err != nil {
		return fmt.Sprintf("its pool could not be read to confirm it still maps %s (%v)", strings.Join(spec.Devices, ", "), err)
	}
	if store.DevicesChanged(spec.Devices, pool.RunnerSettings.Devices) {
		return fmt.Sprintf("pool %s's devices changed after the runner was placed", pool.Name)
	}
	host, err := c.st.GetHost(ctx, hostID)
	if err != nil {
		return fmt.Sprintf("its host could not be read to confirm it still takes the pool (%v)", err)
	}
	if !scheduler.HostSelects(host, pool) {
		return fmt.Sprintf("%s no longer takes pool %s, which maps %s (its labels or its agent changed)", host.Name, pool.Name, strings.Join(pool.RunnerSettings.Devices, ", "))
	}
	return ""
}
