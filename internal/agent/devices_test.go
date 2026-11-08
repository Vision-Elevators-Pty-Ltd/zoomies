package agent

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/eyupio/zoomies/internal/store"
)

// Review of #23, finding 5: every current agent says it maps devices, so the
// scheduler gives a device pool to it and never to an older agent that would
// ignore the field. Only an agent started with agent.devices_only says it is
// reserved for device pools.
func TestTheAgentSaysItMapsDevicesAndWhetherItIsReserved(t *testing.T) {
	a, _, _, _ := newAgent(t, 1)
	if f := a.features(); !slices.Contains(f, FeatureDevices) || slices.Contains(f, FeatureDevicesOnly) {
		t.Fatalf("an ordinary agent advertises %v", f)
	}
	r, _, _, _ := newAgent(t, 1, func(o *Options) { o.DevicesOnly = true })
	if f := r.features(); !slices.Contains(f, FeatureDevices) || !slices.Contains(f, FeatureDevicesOnly) {
		t.Fatalf("a reserved agent advertises %v", f)
	}
}

// The approved design: the runner-emu agent takes only emulator jobs. A create
// of a runner that maps no device is refused on the host, whatever the
// controller sent, and nothing is created.
func TestAReservedAgentRefusesARunnerThatMapsNoDevice(t *testing.T) {
	a, tr, be, _ := newAgent(t, 1, func(o *Options) { o.DevicesOnly = true })
	a.handleCreate(context.Background(), createTask("create", "r"), func() {})
	res := <-tr.results
	if res.OK || !strings.Contains(res.Error, "reserved for pools that map host devices") || res.Fault != store.FaultConfig {
		t.Fatalf("result %+v, want a configuration refusal", res)
	}
	be.mu.Lock()
	defer be.mu.Unlock()
	if len(be.created) != 0 {
		t.Fatal("a general runner was created on the reserved agent")
	}
}
