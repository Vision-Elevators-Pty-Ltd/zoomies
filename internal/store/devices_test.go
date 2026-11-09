package store

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"
)

// A pool may map /dev/kvm only on the container backends and only while its
// host_selector selects kvm=ok; nothing else may be mapped.
func TestAPoolsDevicesNeedTheirHostLabel(t *testing.T) {
	kvm := StringMap{"kvm": "ok"}
	cases := []struct {
		name     string
		devices  []string
		selector StringMap
		backend  BackendKind
		mode     DockerMode
		want     string
	}{
		{"none", nil, nil, BackendDocker, DockerDinD, ""},
		{"kvm on kvm=ok hosts", []string{"/dev/kvm"}, StringMap{"kvm": "ok", "size": "large"}, BackendDocker, DockerNone, ""},
		{"kvm on podman", []string{"/dev/kvm"}, kvm, BackendPodman, DockerNone, ""},
		{"kvm with no selector", []string{"/dev/kvm"}, nil, BackendDocker, DockerNone, "kvm=ok"},
		{"kvm with the wrong value", []string{"/dev/kvm"}, StringMap{"kvm": "yes"}, BackendDocker, DockerNone, "kvm=ok"},
		{"another device", []string{"/dev/sda"}, kvm, BackendDocker, DockerNone, "not a device a pool may map"},
		{"twice", []string{"/dev/kvm", "/dev/kvm"}, kvm, BackendDocker, DockerNone, "named twice"},
		{"the process backend", []string{"/dev/kvm"}, kvm, BackendProcess, DockerNone, "process backend"},
		// Review of #23, finding 3: a privileged sidecar gets every host
		// device, and a host socket can ask the daemon for one.
		{"kvm with docker-in-docker", []string{"/dev/kvm"}, kvm, BackendDocker, DockerDinD, "docker_mode none"},
		{"kvm with the host socket", []string{"/dev/kvm"}, kvm, BackendDocker, DockerHostSocket, "docker_mode none"},
	}
	for _, tc := range cases {
		got := strings.Join(DeviceProblems(tc.devices, tc.selector, tc.backend, tc.mode), "; ")
		if (tc.want == "") != (got == "") || !strings.Contains(got, tc.want) {
			t.Errorf("%s: problems %q, want %q", tc.name, got, tc.want)
		}
	}
	if !(RunnerSettings{Devices: []string{"/dev/kvm"}}).Set() {
		t.Error("a pool that maps a device overrides nothing, says Set")
	}
}

// Devices are compared as sets: order and repetition are not a change.
func TestDevicesChangedComparesSets(t *testing.T) {
	for _, tc := range []struct {
		a, b []string
		want bool
	}{
		{nil, nil, false},
		{nil, []string{}, false},
		{[]string{"/dev/kvm"}, []string{"/dev/kvm"}, false},
		{[]string{"/dev/kvm"}, nil, true},
		{nil, []string{"/dev/kvm"}, true},
		{[]string{"/dev/a", "/dev/b"}, []string{"/dev/b", "/dev/a"}, false},
	} {
		if got := DevicesChanged(tc.a, tc.b); got != tc.want {
			t.Errorf("DevicesChanged(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

// The API marks a change of devices with the zero time; the store stamps it
// with the write's own time, the clock runner rows are created by, and keeps
// it on later writes.
func TestAChangeOfDevicesIsStampedByTheWrite(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	_, p, _ := seedPool(t, s)
	p.RunnerSettings.Devices = []string{"/dev/kvm"}
	p.RunnerSettings.DevicesChangedAt = &time.Time{}
	if err := s.UpdatePool(ctx, p); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetPool(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	at := got.RunnerSettings.DevicesChangedAt
	// Kept to the nanosecond (rows read back to the millisecond): a runner
	// created in the same millisecond reads as older, and is replaced rather
	// than trusted.
	if at == nil || at.IsZero() || !at.Truncate(time.Millisecond).Equal(got.UpdatedAt) {
		t.Fatalf("devices changed at %v, want the write's time %v", at, got.UpdatedAt)
	}
	stamped := *at
	if err := s.UpdatePool(ctx, got); err != nil {
		t.Fatal(err)
	}
	again, _ := s.GetPool(ctx, p.ID)
	if again.RunnerSettings.DevicesChangedAt == nil || !again.RunnerSettings.DevicesChangedAt.Equal(stamped) {
		t.Fatalf("a later write moved the stamp to %v", again.RunnerSettings.DevicesChangedAt)
	}
}

// Review r2 of #23, finding 2: a host reserved for device pools stays
// reserved whatever its agent reports later; only a host that never said so
// is free of it.
func TestAReservationOutlivesTheAgentThatReportedIt(t *testing.T) {
	reserved := []string{"elastic-cpu", FeatureDevices, FeatureDevicesOnly}
	if got := KeepReservation(reserved, []string{"elastic-cpu"}); !slices.Contains(got, FeatureDevicesOnly) || slices.Contains(got, FeatureDevices) {
		t.Fatalf("an older agent's report made the host %v", got)
	}
	if got := KeepReservation([]string{"elastic-cpu"}, []string{"elastic-cpu", FeatureDevices}); slices.Contains(got, FeatureDevicesOnly) {
		t.Fatalf("an ordinary host became reserved: %v", got)
	}
}

// Review r2 of #23, finding 1: a pool that maps devices must be ephemeral,
// have a label of its own, belong to an organisation and name a runner group
// other than Default.
func TestADevicePoolIsBoundToItsOwnJobs(t *testing.T) {
	good := func() *Pool {
		return &Pool{Labels: BrandLabels([]string{"self-hosted", "linux", "x64", "sa-ci-emulator"}), Ephemeral: true,
			RunnerGroup: "emulator", RunnerSettings: RunnerSettings{Devices: []string{"/dev/kvm"}}}
	}
	if got := DevicePoolProblems(good(), TargetOrg); len(got) != 0 {
		t.Fatalf("the emulator pool: %v", got)
	}
	for name, tc := range map[string]struct {
		mut    func(*Pool)
		target TargetType
		want   string
	}{
		"not ephemeral": {func(p *Pool) { p.Ephemeral = false }, TargetOrg, "ephemeral"},
		"no own label":  {func(p *Pool) { p.Labels = StringSlice{"self-hosted", "linux", "x64"} }, TargetOrg, "label of its own"},
		"brand only":    {func(p *Pool) { p.Labels = BrandLabels([]string{"self-hosted", "linux", "x64"}) }, TargetOrg, "label of its own"},
		"default group": {func(p *Pool) { p.RunnerGroup = "Default" }, TargetOrg, "runner group"},
		"no group":      {func(p *Pool) { p.RunnerGroup = "" }, TargetOrg, "runner group"},
		"a repository":  {func(*Pool) {}, TargetRepo, "organisation"},
	} {
		p := good()
		tc.mut(p)
		if got := strings.Join(DevicePoolProblems(p, tc.target), "; "); !strings.Contains(got, tc.want) {
			t.Errorf("%s: problems %q, want %q", name, got, tc.want)
		}
	}
	general := good()
	general.RunnerSettings.Devices = nil
	general.Ephemeral, general.RunnerGroup = false, ""
	if got := DevicePoolProblems(general, TargetRepo); len(got) != 0 {
		t.Fatalf("a pool that maps nothing: %v", got)
	}
}

// Review r2 of #23, finding 1: the pools a claim reads keep their devices, so
// a claim applies the device pool's label rule as the scheduler does.
func TestAClaimSeesAPoolsDevices(t *testing.T) {
	s := newTestStore(t)
	_, p, _ := seedPool(t, s)
	p.RunnerSettings.Devices = []string{"/dev/kvm"}
	if err := s.UpdatePool(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	pools, err := s.ListPoolsForClaim(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pools) != 1 || !pools[0].MapsDevices() {
		t.Fatalf("the claim's pools: %+v", pools)
	}
	if v := ClaimView(p); !v.MapsDevices() {
		t.Fatal("ClaimView dropped the pool's devices")
	}
}
