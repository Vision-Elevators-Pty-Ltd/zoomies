package backend

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// A pool may map a host device into its runner containers
// (runner_settings.devices; store.PoolDevices says which, and which host label
// a pool that maps one must select). Today that is /dev/kvm, for jobs that
// boot an Android emulator.
//
// The agent checks every device on its own host before it creates anything,
// and refuses the runner with an error that says what to fix where the device
// is missing, is not a device, or by its metadata could not be opened by the
// runner. The check is a prediction, not proof: it reads the device node's
// mode and group and the daemon's group map, as the agent's own user, and
// never opens the device in the runner's context. An ACL, an LSM rule, the
// daemon's device cgroup, a runtime that drops supplementary groups (runc
// skips them where setgroups is denied in the namespace) or a KVM module that
// cannot run guests all pass it and fail at the job. A host is certified for a
// device pool by the smoke test in docs/hosts-and-pools.md, run as the
// daemon's user through the daemon itself, never by this check passing. The
// group map is found only beside a rootless Docker socket (rootlesskit's
// state); rootless Podman has none there, so under it only a device every
// user may open passes. The rules it predicts by:
//
//   - a device every user may read and write needs nothing more;
//   - under a daemon that runs as root, the runner is given the device's group
//     as a supplementary group, as the host socket is;
//   - under rootless Docker, the runner runs in the daemon's user namespace,
//     where runc sets the container's own groups and the host user's
//     supplementary groups do not survive (measured on ci-nl-02, 8 Nov 2026:
//     a container of the "runner" account had only its own group, and
//     /dev/kvm, root:kvm 0660 on the host, showed as 65534:65534). Putting the
//     host user in the kvm group is therefore not enough by itself: the
//     device's group must be mapped into the daemon's namespace (an
//     /etc/subgid entry for the user naming that gid, then the user's rootless
//     daemon restarted), and the runner is given the group it is mapped to; or
//     the device must be readable and writable by every user (a udev rule).
//     The map is read from rootlesskit's own state, the child_pid file beside
//     the daemon's socket.

// deviceAccess is how one host device reaches a runner: the groups (as the
// daemon names them) the runner needs to open it.
type deviceAccess struct {
	groups []string
}

// deviceHost is what deviceAccess needs to know about the host, so a test can
// say it.
type deviceHost struct {
	// stat is the device's mode and owning group.
	stat func(path string) (fs.FileMode, int, error)
	// rootless says whether the daemon runs in a user namespace of its own.
	rootless bool
	// gidMap is that namespace's group map; nil where it could not be read.
	gidMap []idRange
}

type idRange struct{ inside, outside, count int64 }

// checkDevices predicts, for every device a spec asks for, whether a runner
// on this host can open it by the device's metadata, and with which groups.
// See the comment above: it does not open the device.
func checkDevices(spec Spec, host deviceHost) (deviceAccess, error) {
	var out deviceAccess
	for _, dev := range spec.Devices {
		mode, gid, err := host.stat(dev)
		if err != nil {
			return deviceAccess{}, fmt.Errorf("backend: pool %q maps %s into its runners, and this host has no %s (%v); only hosts that have it may carry the label its pools select", spec.PoolName, dev, dev, err)
		}
		if mode&fs.ModeDevice == 0 || mode&fs.ModeCharDevice == 0 {
			return deviceAccess{}, fmt.Errorf("backend: pool %q maps %s into its runners, and %s on this host is not a character device", spec.PoolName, dev, dev)
		}
		perm := mode.Perm()
		switch {
		case perm&0o006 == 0o006:
			// Every user may open it.
		case perm&0o060 != 0o060:
			return deviceAccess{}, fmt.Errorf("backend: pool %q maps %s into its runners, and by its mode a runner could not open it: neither its group nor every user may read and write it (mode %s)", spec.PoolName, dev, perm)
		case !host.rootless:
			out.groups = appendGroup(out.groups, strconv.Itoa(gid))
		default:
			inside, ok := translateID(host.gidMap, int64(gid))
			if !ok {
				return deviceAccess{}, fmt.Errorf("backend: pool %q maps %s into its runners, and by its metadata a runner of this rootless daemon could not open it: it is mode %s, group %d, and group %d is not mapped into the daemon's user namespace. Either map it (an /etc/subgid entry \"<user>:%d:1\" for the daemon's user, then restart that user's rootless docker) or let every user open the device (udev: KERNEL==\"kvm\", GROUP=\"kvm\", MODE=\"0666\")", spec.PoolName, dev, perm, gid, gid, gid)
			}
			out.groups = appendGroup(out.groups, strconv.FormatInt(inside, 10))
		}
	}
	return out, nil
}

func appendGroup(groups []string, g string) []string {
	for _, have := range groups {
		if have == g {
			return groups
		}
	}
	return append(groups, g)
}

// translateID is the id inside a user namespace that a host id maps to.
func translateID(m []idRange, outside int64) (int64, bool) {
	for _, r := range m {
		if outside >= r.outside && outside < r.outside+r.count {
			return r.inside + outside - r.outside, true
		}
	}
	return 0, false
}

// parseIDMap reads /proc/<pid>/gid_map: "inside outside count" per line.
func parseIDMap(data string) ([]idRange, error) {
	var out []idRange
	for _, line := range strings.Split(strings.TrimSpace(data), "\n") {
		f := strings.Fields(line)
		if len(f) == 0 {
			continue
		}
		if len(f) != 3 {
			return nil, fmt.Errorf("an id map line %q is not three numbers", line)
		}
		var n [3]int64
		for i, s := range f {
			v, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("an id map line %q is not three numbers", line)
			}
			n[i] = v
		}
		out = append(out, idRange{inside: n[0], outside: n[1], count: n[2]})
	}
	return out, nil
}

// rootlessGIDMap is the group map of the rootless daemon serving socket,
// read from rootlesskit's state directory beside it (dockerd-rootless.sh's
// default, $XDG_RUNTIME_DIR/dockerd-rootless/child_pid). An error where there
// is none to read.
func rootlessGIDMap(socket string) ([]idRange, error) {
	socket = strings.TrimPrefix(socket, "unix://")
	if socket == "" {
		return nil, fmt.Errorf("the daemon has no socket path")
	}
	pid, err := os.ReadFile(filepath.Join(filepath.Dir(socket), "dockerd-rootless", "child_pid"))
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile("/proc/" + strings.TrimSpace(string(pid)) + "/gid_map")
	if err != nil {
		return nil, err
	}
	return parseIDMap(string(data))
}

// devicesOnThisHost is checkDevices against this agent's host and daemon: a
// prediction from metadata, which a passing smoke test on the host is what
// certifies.
func (b *DockerBackend) devicesOnThisHost(spec Spec) (deviceAccess, error) {
	if len(spec.Devices) == 0 {
		return deviceAccess{}, nil
	}
	socket := b.api.SocketPath()
	host := deviceHost{stat: func(path string) (fs.FileMode, int, error) {
		if _, err := os.Stat(path); err != nil {
			return 0, 0, err
		}
		mode, gid, ok := statOwner(path)
		if !ok {
			return 0, 0, fmt.Errorf("could not read its owner")
		}
		return mode, gid, nil
	}}
	if m, err := rootlessGIDMap(socket); err == nil {
		host.rootless, host.gidMap = true, m
	} else if IsRootlessEndpoint(socket) {
		host.rootless = true // and no map: only a device every user may open
	}
	return checkDevices(spec, host)
}

// deviceMappings is the HostConfig.Devices entry for each device a spec asks
// for, at the same path inside the container.
func deviceMappings(devices []string) []DeviceMapping {
	if len(devices) == 0 {
		return nil
	}
	out := make([]DeviceMapping, 0, len(devices))
	for _, d := range devices {
		out = append(out, DeviceMapping{PathOnHost: d, PathInContainer: d, CgroupPermissions: "rwm"})
	}
	return out
}
