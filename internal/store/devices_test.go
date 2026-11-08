package store

import (
	"context"
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
