package scheduler

import (
	"strings"
	"testing"

	"github.com/eyupio/zoomies/internal/store"
)

// sharedHost is a measured host whose agent can hold a docker-in-docker pair to
// its typed size between the two containers.
func sharedHost(id string, capacity, cpus int, memoryMB int64) *store.Host {
	h := sized(id, capacity, cpus, memoryMB, 500*1024)
	h.Features = store.StringSlice{store.FeatureDinDSharedLimit}
	return h
}

// sharedPool is a typed docker-in-docker pool that asks for the shared limit.
func sharedPool(name string, cpus float64, memoryMB int64) *store.Pool {
	p := limited(name, cpus, memoryMB)
	p.DockerMode = store.DockerDinD
	p.Resources.DinDSharedLimit = true
	return p
}

// The point of the setting: a pair whose parent cgroup holds it to the typed
// size can never use more than that between its two containers, so charging the
// host for both is booking room nothing can occupy. It is charged once.
func TestASharedLimitPairIsChargedItsTypedSizeOnce(t *testing.T) {
	h := sharedHost("host_a", 8, 24, 32*1024)
	p := sharedPool("shared", 4, 8*1024)

	got := Reserve(p, h)
	if got.CPUs != 4 || got.MemoryMB != 8*1024 {
		t.Fatalf("Reserve = %+v; want 4 CPU and 8192 MB, the typed size once", got)
	}
	r := &store.Runner{AllocatedCPUs: 4, AllocatedMemoryMB: 8 * 1024, AllocationSource: store.AllocationFromPool}
	if g := RunnerGuarantee(p, h, r); g.CPUs != 4 || g.MemoryMB != 8*1024 {
		t.Fatalf("RunnerGuarantee = %+v; want the typed size once", g)
	}
	if mb, ok := LaunchedMemoryMB(p, h, r); !ok || mb != 8*1024 {
		t.Fatalf("LaunchedMemoryMB = %d, %v; want 8192", mb, ok)
	}
}

// Off is upstream: the same pool without the setting is charged for both
// containers, on the same host, so nothing has changed for a fleet that does not
// ask for it.
func TestWithoutTheSharedLimitATypedPairIsStillChargedTwice(t *testing.T) {
	h := sharedHost("host_a", 8, 24, 32*1024)
	p := sharedPool("doubled", 4, 8*1024)
	p.Resources.DinDSharedLimit = false

	if got := Reserve(p, h); got.CPUs != 8 || got.MemoryMB != 16*1024 {
		t.Fatalf("Reserve = %+v; want 8 CPU and 16384 MB, both containers in full", got)
	}
	r := &store.Runner{AllocatedCPUs: 4, AllocatedMemoryMB: 8 * 1024, AllocationSource: store.AllocationFromPool}
	if g := RunnerGuarantee(p, h, r); g.CPUs != 8 || g.MemoryMB != 16*1024 {
		t.Fatalf("RunnerGuarantee = %+v; want both containers in full", g)
	}
	if mb, _ := LaunchedMemoryMB(p, h, r); mb != 16*1024 {
		t.Fatalf("LaunchedMemoryMB = %d; want 16384", mb)
	}
}

// An agent that predates the setting ignores it and gives each container the
// whole figure, so a host it runs on is charged for both whatever the pool says.
// Charging it once would book half of what that host's containers may take.
func TestASharedLimitPairOnAnAgentThatCannotHoldItIsChargedTwice(t *testing.T) {
	h := sized("host_old", 8, 24, 32*1024, 500*1024)
	p := sharedPool("shared", 4, 8*1024)

	if got := Reserve(p, h); got.CPUs != 8 || got.MemoryMB != 16*1024 {
		t.Fatalf("Reserve = %+v on a host without the feature; want both containers in full", got)
	}
}

// What it buys, counted the way an operator would: a host that held two typed
// pairs at the doubled charge holds four at the shared one.
func TestASharedLimitHostHoldsTwiceAsManyTypedPairs(t *testing.T) {
	memory := hostFor(32 * 1024)
	doubled := sharedPool("doubled", 2, 8*1024)
	doubled.Resources.DinDSharedLimit = false
	hs := newHostSet([]*store.Host{sharedHost("host_a", 8, 24, memory)}, []*store.Pool{doubled}, nil, now)
	if got := hs.place(doubled, 8); len(got) != 2 {
		t.Fatalf("placed %d doubled pairs of 8 GB on a 32 GB host, want 2", len(got))
	}

	shared := sharedPool("shared", 2, 8*1024)
	hs = newHostSet([]*store.Host{sharedHost("host_b", 8, 24, memory)}, []*store.Pool{shared}, nil, now)
	if got := hs.place(shared, 8); len(got) != 4 {
		t.Fatalf("placed %d shared pairs of 8 GB on a 32 GB host, want 4", len(got))
	}
}

// A field the host sized is one slot the pair already splits, so the setting
// has nothing to halve there: a pool that typed only its memory is charged its
// memory once and one slot of CPU, as it would be anyway.
func TestASharedLimitLeavesAHostSizedFieldAlone(t *testing.T) {
	h := sharedHost("host_a", 4, 16, 32*1024)
	p := sharedPool("memory-only", 0, 6*1024)
	without := sharedPool("memory-only", 0, 6*1024)
	without.Resources.DinDSharedLimit = false

	got, base := Reserve(p, h), Reserve(without, h)
	if got.MemoryMB != 6*1024 || base.MemoryMB != 12*1024 {
		t.Fatalf("memory charged %d shared and %d not; want 6144 and 12288", got.MemoryMB, base.MemoryMB)
	}
	if got.CPUs != base.CPUs {
		t.Fatalf("the host-sized CPU is charged %v shared and %v not; want the same slot", got.CPUs, base.CPUs)
	}
}

// A reduced runner of a shared pool is given what the host can spare as one
// figure for the pair, not halved for a second container that the parent
// cgroup has no room for.
func TestAReducedSharedLimitRunnerIsGivenWhatIsLeftWhole(t *testing.T) {
	h := sharedHost("host_a", 8, 24, 32*1024)
	p := sharedPool("shared", 4, 8*1024)
	p.Resources.MinMemoryMB = 4 * 1024
	left := Reservation{CPUs: 24, MemoryMB: 6 * 1024, DiskMB: 100 * 1024}

	charge, grant, ok := ReducedSize(p, h, left, h.Allocatable())
	if !ok {
		t.Fatal("a shared pool with a 4 GB minimum was refused 6 GB")
	}
	if charge.MemoryMB != 6*1024 || grant.MemoryMB != 6*1024 {
		t.Fatalf("charge %d MB, grant %d MB; want 6144 for both", charge.MemoryMB, grant.MemoryMB)
	}
}

// The shortfall says the charge is doubled only where it is.
func TestTheShortfallDoesNotCallASharedChargeDoubled(t *testing.T) {
	h := sharedHost("host_a", 8, 12, 40*1024)
	p := sharedPool("builders", 16, 0)

	got := HostShortfall(h, p)
	if got == "" {
		t.Fatal("a 16 CPU pool fits a 12 CPU host")
	}
	if strings.Contains(got, "twice") || !strings.Contains(got, "charged 16") {
		t.Fatalf("HostShortfall = %q; want the typed 16 CPU charged once", got)
	}
}
