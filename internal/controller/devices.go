package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/eyupio/zoomies/internal/agent"
	"github.com/eyupio/zoomies/internal/backend"
	"github.com/eyupio/zoomies/internal/github"
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

// gateDeviceCreates is tasks less every create whose device policy no longer
// holds: a device-mapping runner its pool or host no longer allows, and a
// runner placed without devices for a pool that has since gained them, or on a
// host since reserved for device pools. Each refused create's lease is cleared
// and its runner failed with the reason, so the reap cleans it (and its
// registration) up like any runner that failed to start. Every create is
// checked, at the cost of two primary-key reads; other tasks pass unread.
func (c *Controller) gateDeviceCreates(ctx context.Context, hostID string, q *taskQueue, tasks []agent.Task) []agent.Task {
	out := tasks[:0:0]
	for _, t := range tasks {
		if t.Kind != agent.TaskCreateRunner || t.Spec == nil {
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

// deviceCreateRefusal says why a create of a runner may not go to hostID now,
// or "" where it may. A runner that maps spec.Devices needs the pool still to
// map exactly those devices and the host still to take it
// (scheduler.HostSelects: its labels, and a current agent reserved for device
// pools); a pool or host that cannot be read then refuses, the safe answer for
// a runner that would be given a host device. A runner that maps nothing is
// refused only where that is now known to be wrong: its pool has since gained
// devices, or its host has since been reserved for device pools. Where its
// pool or host cannot be read it is delivered, as it always was.
func (c *Controller) deviceCreateRefusal(ctx context.Context, hostID string, spec *backend.Spec) string {
	if len(spec.Devices) == 0 {
		if pool, err := c.st.GetPool(ctx, spec.PoolID); err == nil && pool.MapsDevices() {
			return fmt.Sprintf("pool %s's devices changed after the runner was placed: it now maps %s", pool.Name, strings.Join(pool.RunnerSettings.Devices, ", "))
		}
		if host, err := c.st.GetHost(ctx, hostID); err == nil && host.Supports(store.FeatureDevicesOnly) {
			return fmt.Sprintf("%s is reserved for pools that map devices", host.Name)
		}
		return ""
	}
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

// errDeviceGroup refuses a device pool's registration where GitHub would
// offer its runner to jobs it must not run.
var errDeviceGroup = errors.New("a pool that maps devices must register in an organisation runner group restricted to selected workflows")

// deviceGroupRefusal is why a runner of a pool that maps devices must not be
// registered in group, or "" where it may. The scheduler places only jobs
// that ask for the pool's own labels on it (scheduler.PoolMatches), but GitHub
// hands a registered runner any queued job its labels satisfy, and every
// ephemeral runner carries self-hosted, linux and x64: so the runner must
// belong to an organisation runner group GitHub restricts to selected
// workflows (the emulator ones), with no public repositories, and never the
// Default group. Checked before a credential is minted, every time.
func deviceGroupRefusal(inst *store.Installation, pool *store.Pool, group github.RunnerGroup, unresolved string) string {
	switch {
	case inst.TargetType != store.TargetOrg:
		return fmt.Sprintf("%s is a repository, which has no runner groups to restrict", inst.Target)
	case unresolved != "":
		return unresolved
	case group.ID == 0 || group.Default:
		return fmt.Sprintf("pool %s registers in the Default group", pool.Name)
	case !group.RestrictedToWorkflows:
		return fmt.Sprintf("runner group %s is not restricted to selected workflows on GitHub", group.Name)
	case !group.PublicRepositoryAccessKnown || group.AllowsPublicRepositories:
		return fmt.Sprintf("runner group %s may serve public repositories, or GitHub did not say", group.Name)
	}
	return ""
}
