package store

import (
	"strings"
	"testing"
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
		want     string
	}{
		{"none", nil, nil, BackendDocker, ""},
		{"kvm on kvm=ok hosts", []string{"/dev/kvm"}, StringMap{"kvm": "ok", "size": "large"}, BackendDocker, ""},
		{"kvm on podman", []string{"/dev/kvm"}, kvm, BackendPodman, ""},
		{"kvm with no selector", []string{"/dev/kvm"}, nil, BackendDocker, "kvm=ok"},
		{"kvm with the wrong value", []string{"/dev/kvm"}, StringMap{"kvm": "yes"}, BackendDocker, "kvm=ok"},
		{"another device", []string{"/dev/sda"}, kvm, BackendDocker, "not a device a pool may map"},
		{"twice", []string{"/dev/kvm", "/dev/kvm"}, kvm, BackendDocker, "named twice"},
		{"the process backend", []string{"/dev/kvm"}, kvm, BackendProcess, "process backend"},
	}
	for _, tc := range cases {
		got := strings.Join(DeviceProblems(tc.devices, tc.selector, tc.backend), "; ")
		if (tc.want == "") != (got == "") || !strings.Contains(got, tc.want) {
			t.Errorf("%s: problems %q, want %q", tc.name, got, tc.want)
		}
	}
	if !(RunnerSettings{Devices: []string{"/dev/kvm"}}).Set() {
		t.Error("a pool that maps a device overrides nothing, says Set")
	}
}
