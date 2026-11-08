package scheduler

import (
	"strings"
	"testing"
	"time"

	"github.com/eyupio/zoomies/internal/store"
)

// kvmPool is a pool that maps /dev/kvm, as approved for the emulator jobs:
// pool: pr, kvm: ok, docker_mode none.
func kvmPool() *store.Pool {
	p := testPool("emulator", "emulator")
	p.DockerMode = store.DockerNone
	p.HostSelector = store.StringMap{"pool": "pr", "kvm": "ok"}
	p.RunnerSettings.Devices = []string{"/dev/kvm"}
	return p
}

// emuHost is the runner-emu principal's agent: kvm=ok, an agent that maps
// devices, reserved for device pools.
func emuHost(id string) *store.Host {
	h := testHost(id, 4, 0)
	h.Labels = store.StringMap{"pool": "pr", "kvm": "ok"}
	h.Features = store.StringSlice{store.FeatureDevices, store.FeatureDevicesOnly}
	return h
}

// Review of #23, findings 5 and the approved design: a pool that maps devices
// goes only to an agent that says it maps them (an older agent would ignore
// the field and start a runner without the device), and an agent reserved for
// device pools takes no other pool, however broad its selector.
func TestOnlyDeviceAgentsTakeDevicePoolsAndReservedAgentsTakeNothingElse(t *testing.T) {
	general := testPool("pr")
	general.HostSelector = store.StringMap{"pool": "pr"}
	old := testHost("old", 4, 0) // kvm=ok, but its agent predates devices
	old.Labels = store.StringMap{"pool": "pr", "kvm": "ok"}
	plain := testHost("plain", 4, 0)
	plain.Labels = store.StringMap{"pool": "pr"}
	plain.Features = store.StringSlice{store.FeatureDevices} // a current agent, not reserved
	unlabelled := emuHost("unlabelled")
	unlabelled.Labels = store.StringMap{"pool": "pr"}
	for _, tc := range []struct {
		host *store.Host
		pool *store.Pool
		want bool
	}{
		{emuHost("emu"), kvmPool(), true},
		{emuHost("emu"), general, false},
		{old, kvmPool(), false},
		{old, general, true},
		{plain, general, true},
		{plain, kvmPool(), false}, // no kvm=ok label
		{unlabelled, kvmPool(), false},
		{unlabelled, general, false},
	} {
		if got := HostSelects(tc.host, tc.pool); got != tc.want {
			t.Errorf("HostSelects(%s, %s) = %v, want %v", tc.host.Name, tc.pool.Name, got, tc.want)
		}
	}

	// Placement: the general pool's demand goes to the plain host only, and
	// the emulator pool's to the reserved agent only.
	general.MinRunners, general.MaxRunners = 3, 3
	emu := kvmPool()
	emu.MinRunners, emu.MaxRunners = 2, 2
	plan := Decide(snap([]*store.Pool{general, emu}, nil, nil, []*store.Host{plain, emuHost("emu"), old}))
	for _, pp := range plan.Pools {
		for _, a := range actionsOf(pp.Actions, ActionCreate) {
			switch {
			case pp.PoolID == general.ID && a.HostID == "emu":
				t.Fatalf("a general runner was placed on the reserved emulator agent: %+v", a)
			case pp.PoolID == emu.ID && a.HostID != "emu":
				t.Fatalf("an emulator runner was placed on %s: %+v", a.HostID, a)
			}
		}
	}

	// Only an older agent has the label: the pool says why it cannot be placed.
	emu = kvmPool()
	emu.MinRunners = 1
	pp := only(t, Decide(snap([]*store.Pool{emu}, nil, nil, []*store.Host{old})))
	if !strings.Contains(pp.Reason, "cannot map the pool's devices") {
		t.Fatalf("reason %q does not say the agent cannot map devices", pp.Reason)
	}
}

// Review of #23, finding 1: a container keeps the devices it was created
// with, so a runner made before its pool's devices changed is replaced as soon
// as it is not running a job, whichever way the devices changed.
func TestRunnersMadeBeforeTheDevicesChangedAreReplaced(t *testing.T) {
	changed := ago(30 * time.Second)
	for _, tc := range []struct {
		name    string
		pool    func() *store.Pool
		state   store.RunnerState
		age     time.Duration
		drained bool
	}{
		{"devices cleared, an idle runner", clearedPool, store.RunnerIdle, time.Minute, true},
		{"devices cleared, a runner still registering", clearedPool, store.RunnerRegistering, time.Minute, true},
		{"devices cleared, a busy runner keeps its job", clearedPool, store.RunnerBusy, time.Minute, false},
		{"devices cleared, a runner made since", clearedPool, store.RunnerIdle, 10 * time.Second, false},
		{"devices added to a warm pool, an idle runner", kvmPool, store.RunnerIdle, time.Minute, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := tc.pool()
			p.RunnerSettings.DevicesChangedAt = &changed
			p.IdleTimeout, p.MinRunners = 0, 1 // not surplus
			r := testRunner("r1", p, tc.state, tc.age)
			host := testHost("host_a", 4, 1)
			if p.MapsDevices() {
				host = emuHost("host_a")
			}
			drains := actionsOf(only(t, Decide(snap([]*store.Pool{p}, []*store.Runner{r}, nil, []*store.Host{host}))).Actions, ActionDrain)
			if tc.drained != (len(drains) == 1) {
				t.Fatalf("drains = %+v, want drained %v", drains, tc.drained)
			}
			if tc.drained && !strings.Contains(drains[0].Reason, "devices changed") {
				t.Fatalf("reason %q", drains[0].Reason)
			}
		})
	}
}

// clearedPool is an emulator pool whose devices and kvm selector were cleared.
func clearedPool() *store.Pool {
	p := testPool("emulator", "emulator")
	p.DockerMode = store.DockerNone
	p.HostSelector = store.StringMap{}
	return p
}

// Review of #23, finding 4: removing kvm=ok from a host (or swapping its agent
// for one that cannot map devices) leaves its device runners on a host the
// pool no longer takes; the idle ones are replaced, the busy ones finish their
// job. A general runner on a host since reserved for device pools goes too.
// Runners of a pool that maps nothing, on a host that is not reserved, are
// never touched by this.
func TestRunnersOnAHostTheirDevicePoolNoLongerTakesAreReplaced(t *testing.T) {
	unlabelled := emuHost("host_a")
	delete(unlabelled.Labels, "kvm")
	oldAgent := emuHost("host_a")
	oldAgent.Features = nil
	for _, tc := range []struct {
		name    string
		pool    *store.Pool
		host    *store.Host
		state   store.RunnerState
		drained string
	}{
		{"kvm label removed, an idle runner", kvmPool(), unlabelled, store.RunnerIdle, "no longer takes it"},
		{"kvm label removed, a busy runner keeps its job", kvmPool(), unlabelled, store.RunnerBusy, ""},
		{"agent no longer maps devices", kvmPool(), oldAgent, store.RunnerIdle, "no longer takes it"},
		{"still labelled", kvmPool(), emuHost("host_a"), store.RunnerIdle, ""},
		{"a general runner on a reserved host", testPool("general"), emuHost("host_a"), store.RunnerIdle, "reserved for pools that map devices"},
		{"a general runner on a general host", testPool("general"), testHost("host_a", 4, 1), store.RunnerIdle, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.pool.IdleTimeout, tc.pool.MinRunners = 0, 1 // not surplus
			r := testRunner("r1", tc.pool, tc.state, time.Minute)
			drains := actionsOf(only(t, Decide(snap([]*store.Pool{tc.pool}, []*store.Runner{r}, nil, []*store.Host{tc.host}))).Actions, ActionDrain)
			if tc.drained == "" {
				if len(drains) != 0 {
					t.Fatalf("drained %+v", drains)
				}
				return
			}
			if len(drains) != 1 || !strings.Contains(drains[0].Reason, tc.drained) {
				t.Fatalf("drains = %+v, want one saying %q", drains, tc.drained)
			}
		})
	}
}
