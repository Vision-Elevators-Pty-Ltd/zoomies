package backend

import (
	"errors"
	"io/fs"
	"strings"
	"testing"
	"time"

	"github.com/eyupio/zoomies/internal/store"
)

// /dev/kvm as ci-nl-01 and ci-nl-02 have it: crw-rw---- root kvm (gid 994).
const kvmMode = fs.ModeDevice | fs.ModeCharDevice | 0o660

func kvmHost(mode fs.FileMode, gid int, rootless bool, m []idRange) deviceHost {
	return deviceHost{stat: func(string) (fs.FileMode, int, error) { return mode, gid, nil }, rootless: rootless, gidMap: m}
}

// ci-nl-02's rootless daemon of the "runner" account, read from its
// rootlesskit child on 8 Nov 2026: its own gid at 0, its subgid range from 1.
var ciNL02Map = []idRange{{inside: 0, outside: 1002, count: 1}, {inside: 1, outside: 231072, count: 65536}}

func kvmSpec() Spec {
	s := jitSpec()
	s.PoolName = "emulator"
	s.Devices = []string{"/dev/kvm"}
	return s
}

// Which groups a runner is given to open /dev/kvm, and when the agent refuses
// it, saying what to fix.
func TestTheAgentDecidesHowARunnerOpensItsDevices(t *testing.T) {
	withKVM := append(append([]idRange(nil), ciNL02Map...), idRange{inside: 65537, outside: 994, count: 1})
	cases := []struct {
		name   string
		host   deviceHost
		groups []string
		err    string
	}{
		{"every user may open it: nothing more", kvmHost(fs.ModeDevice|fs.ModeCharDevice|0o666, 994, true, ciNL02Map), nil, ""},
		{"a root daemon: the device's group", kvmHost(kvmMode, 994, false, nil), []string{"994"}, ""},
		{"rootless with kvm mapped: the group it is mapped to", kvmHost(kvmMode, 994, true, withKVM), []string{"65537"}, ""},
		{"rootless as ci-nl-02 is today: refused, saying both fixes", kvmHost(kvmMode, 994, true, ciNL02Map), nil, "/etc/subgid"},
		{"rootless with no map to read: refused", kvmHost(kvmMode, 994, true, nil), nil, "not mapped"},
		{"not a character device", kvmHost(0o666, 0, false, nil), nil, "not a character device"},
		{"its group may not open it either", kvmHost(fs.ModeDevice|fs.ModeCharDevice|0o600, 994, false, nil), nil, "neither its group"},
		{"a host without it, as sa-ci-01", deviceHost{stat: func(string) (fs.FileMode, int, error) { return 0, 0, errors.New("no such file or directory") }}, nil, "this host has no /dev/kvm"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := checkDevices(kvmSpec(), tc.host)
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) || !strings.Contains(err.Error(), "emulator") {
					t.Fatalf("err = %v, want one naming the pool and %q", err, tc.err)
				}
				return
			}
			if err != nil || strings.Join(got.groups, ",") != strings.Join(tc.groups, ",") {
				t.Fatalf("groups = %v, %v; want %v", got.groups, err, tc.groups)
			}
		})
	}
	if got, err := checkDevices(jitSpec(), kvmHost(0, 0, false, nil)); err != nil || got.groups != nil {
		t.Fatalf("a pool without devices: %+v, %v", got, err)
	}
}

func TestAnIDMapIsReadAndTranslated(t *testing.T) {
	m, err := parseIDMap("         0       1002          1\n         1     231072      65536\n")
	if err != nil || len(m) != 2 || m[1] != (idRange{1, 231072, 65536}) {
		t.Fatalf("map = %+v, %v", m, err)
	}
	if in, ok := translateID(m, 231072+999); !ok || in != 1000 {
		t.Fatalf("subgid 232071 -> %d, %v; want 1000", in, ok)
	}
	if _, ok := translateID(m, 994); ok {
		t.Fatal("an unmapped gid was translated")
	}
	if _, err := parseIDMap("1 2\n"); err == nil {
		t.Fatal("a short line was accepted")
	}
}

// The runner container, and only it, gets the device, read-write, with the
// groups the check decided; a pool without devices gets neither.
func TestOnlyTheRunnerIsGivenTheDeviceAndItsGroups(t *testing.T) {
	spec := kvmSpec()
	spec.DockerMode = store.DockerDinD
	o := containerOptions{Now: time.Now(), DeviceGroups: []string{"65537"}}
	hc := buildRunnerConfig(spec, dockerFlavor(), o).HostConfig
	if len(hc.Devices) != 1 || hc.Devices[0] != (DeviceMapping{PathOnHost: "/dev/kvm", PathInContainer: "/dev/kvm", CgroupPermissions: "rwm"}) {
		t.Fatalf("devices = %+v", hc.Devices)
	}
	if len(hc.GroupAdd) != 1 || hc.GroupAdd[0] != "65537" {
		t.Fatalf("groups = %v", hc.GroupAdd)
	}
	if side := buildDinDConfig(spec, dockerFlavor(), o).HostConfig; len(side.Devices) != 0 {
		t.Fatalf("the sidecar was given %+v", side.Devices)
	}
	plain := buildRunnerConfig(jitSpec(), dockerFlavor(), containerOptions{Now: time.Now()}).HostConfig
	if len(plain.Devices) != 0 || len(plain.GroupAdd) != 0 {
		t.Fatalf("a pool without devices got %+v %v", plain.Devices, plain.GroupAdd)
	}
}

// A spec may name only a device a pool may map, and the process backend,
// which has no container, refuses any.
func TestOnlyKnownDevicesReachABackend(t *testing.T) {
	spec := jitSpec()
	spec.Devices = []string{"/dev/sda"}
	if err := spec.Validate(); err == nil {
		t.Fatal("/dev/sda was accepted")
	}
	spec.Devices = []string{"/dev/kvm"}
	if err := spec.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := (&ProcessBackend{}).CreateWithResult(t.Context(), spec); err == nil || !strings.Contains(err.Error(), "process backend") {
		t.Fatalf("the process backend: %v", err)
	}
}
