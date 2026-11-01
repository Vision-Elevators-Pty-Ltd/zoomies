package backend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"os/user"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/eyupio/zoomies/internal/store"
)

// A shared limit (store.Resources.DinDSharedLimit) holds a docker-in-docker
// runner and its sidecar to the pool's typed size between them, where the
// default gives each container the whole figure and charges the host twice.
//
// It is a parent cgroup. Both containers are created with CgroupParent naming a
// systemd slice of their own, zoomies-<runner>.slice, and that slice is given
// MemoryMax and CPUQuota at the typed size before either container exists. Each
// container keeps its own limit as well, no higher than the slice's, so neither
// half is ever unbounded and the pair together cannot pass the slice.
//
// The slice is set through systemctl rather than by writing memory.max into the
// cgroup directory. systemd owns every cgroup under a slice it manages and
// writes its configured values back whenever it realises the unit again (a
// reload, a sibling's property change), so a figure written behind its back is
// one that can quietly become "max". set-property --runtime is systemd's own
// record of the limit, applied to the slice the moment the first container's
// scope is started in it, and gone at the next boot.
//
// On a rootless daemon the slice belongs to the daemon user's own manager
// (user@UID.service), which is where a rootless daemon's systemd cgroup driver
// puts its scopes and which is delegated the cpu and memory controllers on any
// host where per-container limits work at all. The agent reaches it with
// systemctl --user, as that user, or as root with --machine=<user>@.host. A
// rootful daemon's slice is the system manager's, which needs a root agent.
//
// The cgroupfs driver is refused rather than worked around: the parent would be
// a directory the agent must create and own, which a rootless agent cannot on
// cgroup v2, and a pool that books its pairs once must not run them unbounded.

// LabelSharedSlice names the slice a shared-limit pair was placed under, on
// both of its containers, so that removing the runner can let it go.
const LabelSharedSlice = LabelPrefix + "shared-slice"

// sharedSlicePrefix is what every shared-limit slice is named under. The dash
// makes zoomies.slice their common parent, which keeps them together in
// systemd-cgls and gives an operator one unit to look at.
const sharedSlicePrefix = "zoomies-"

// sharedSliceUnit is the slice unit names Release will act on. A label is read
// back from a container before release, and anything not of this shape is not
// passed to systemctl.
var sharedSliceUnit = regexp.MustCompile(`^zoomies-[A-Za-z0-9_]{1,64}\.slice$`)

// sharedSliceName is the slice a runner's pair is placed under. The runner ID
// is unique for the runner's life; a dash in a slice name is a level of the
// hierarchy, so anything other than a letter, digit or underscore becomes an
// underscore and the slice is one level below zoomies.slice.
func sharedSliceName(spec Spec) string {
	id := firstNonEmpty(strings.TrimSpace(spec.RunnerID), strings.TrimSpace(spec.Name))
	var b strings.Builder
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
		if b.Len() == 64 {
			break
		}
	}
	if b.Len() == 0 {
		b.WriteString("runner")
	}
	return sharedSlicePrefix + b.String() + ".slice"
}

// sharedLimits is what the slice over a pair holds the two to together. A field
// each half is given whole (one the pool typed, or every field of a spec that
// says nothing about where its limits came from) is that figure once: it is the
// size the scheduler charged. A field the host sized is already one slot the
// pair splits, so the slice is the sum of the halves, which is the slot.
func sharedLimits(spec Spec) store.Resources {
	runner, daemon := pairLimits(spec)
	out := store.Resources{}
	whole := spec.ResourcesSource != store.AllocationFromHost
	if whole || spec.CPUTyped {
		out.CPUs = spec.Resources.CPUs
	} else if runner.CPUs > 0 && daemon.CPUs > 0 {
		out.CPUs = runner.CPUs + daemon.CPUs
	}
	if whole || spec.MemoryTyped {
		out.MemoryMB = spec.Resources.MemoryMB
	} else if runner.MemoryMB > 0 && daemon.MemoryMB > 0 {
		out.MemoryMB = runner.MemoryMB + daemon.MemoryMB
	}
	return out
}

// sliceLimiter sets and lets go of a slice's limits. systemctlSlices is the
// real one; a test supplies its own.
type sliceLimiter interface {
	Limit(ctx context.Context, unit string, res store.Resources) error
	Release(ctx context.Context, unit string) error
}

// errSharedLimitUnsupported is a daemon a shared limit cannot be applied on.
var errSharedLimitUnsupported = errors.New("backend: a shared docker-in-docker limit cannot be applied on this daemon")

// runCommand runs one command with extra environment and returns its combined
// output. It is a variable so a test can see the command without running it.
type runCommand func(ctx context.Context, env []string, name string, args ...string) ([]byte, error)

func execCommand(ctx context.Context, env []string, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = append(os.Environ(), env...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	return out.Bytes(), err
}

// systemctlSlices drives one systemd manager's slices with systemctl.
type systemctlSlices struct {
	// args select the manager: none for the system's, --user (and --machine
	// for another user's) for a rootless daemon's.
	args []string
	env  []string
	run  runCommand
}

// Limit gives the slice its memory and CPU ceilings. CPU is rounded up to a
// whole percent of a core, which is the form every systemd that has CPUQuota
// accepts; the containers' own quotas below it are exact.
func (s systemctlSlices) Limit(ctx context.Context, unit string, res store.Resources) error {
	if !sharedSliceUnit.MatchString(unit) {
		return fmt.Errorf("backend: %q is not a shared-limit slice name", unit)
	}
	var props []string
	if res.MemoryMB > 0 {
		props = append(props, "MemoryMax="+strconv.FormatInt(res.MemoryMB*1024*1024, 10))
	}
	if res.CPUs > 0 {
		props = append(props, fmt.Sprintf("CPUQuota=%d%%", int64(math.Ceil(res.CPUs*100-1e-9))))
	}
	if len(props) == 0 {
		return nil
	}
	args := append(append(append([]string(nil), s.args...), "set-property", "--runtime", unit), props...)
	if out, err := s.run(ctx, s.env, "systemctl", args...); err != nil {
		return fmt.Errorf("backend: setting the shared limit on %s with systemctl %s: %w: %s",
			unit, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

// Release stops the slice, which by now holds nothing, and drops the runtime
// properties Limit wrote. Both are best effort: a slice that was never started
// has nothing to stop, and what is left behind is a few bytes under /run that
// name a runner that will not come back.
func (s systemctlSlices) Release(ctx context.Context, unit string) error {
	if !sharedSliceUnit.MatchString(unit) {
		return fmt.Errorf("backend: %q is not a shared-limit slice name", unit)
	}
	var errs []error
	for _, verb := range []string{"stop", "revert"} {
		args := append(append([]string(nil), s.args...), verb, unit)
		if out, err := s.run(ctx, s.env, "systemctl", args...); err != nil {
			errs = append(errs, fmt.Errorf("systemctl %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out))))
		}
	}
	return errors.Join(errs...)
}

// userSocketUID is the user a /run/user/<uid>/... socket belongs to, which is
// how a rootless daemon's socket is laid out.
var userSocketUID = regexp.MustCompile(`^/run/user/(\d+)/`)

// systemdSlicesFor is the slice manager for the daemon info describes, or why
// there is none.
func systemdSlicesFor(info SystemInfo, socket string, euid int, getenv func(string) string, lookup func(uid string) (string, error), run runCommand) (sliceLimiter, error) {
	if !strings.EqualFold(strings.TrimSpace(info.CgroupDriver), "systemd") {
		return nil, fmt.Errorf("%w: the daemon uses the %q cgroup driver, and a shared limit is a systemd slice over the pair; run the daemon with the systemd cgroup driver (native.cgroupdriver=systemd), or clear resources.dind_shared_limit on the pool",
			errSharedLimitUnsupported, info.CgroupDriver)
	}
	if !IsRootless(info) {
		if euid != 0 {
			return nil, fmt.Errorf("%w: the daemon is rootful, so its slices belong to the system manager, which only root may change; run the agent as root, use a rootless daemon, or clear resources.dind_shared_limit on the pool",
				errSharedLimitUnsupported)
		}
		return systemctlSlices{run: run}, nil
	}
	owner := euid
	if m := userSocketUID.FindStringSubmatch(socket); m != nil {
		if uid, err := strconv.Atoi(m[1]); err == nil {
			owner = uid
		}
	}
	if owner == euid {
		env := []string{}
		if getenv("XDG_RUNTIME_DIR") == "" {
			env = append(env, fmt.Sprintf("XDG_RUNTIME_DIR=/run/user/%d", euid))
		}
		return systemctlSlices{args: []string{"--user"}, env: env, run: run}, nil
	}
	if euid != 0 {
		return nil, fmt.Errorf("%w: the daemon belongs to uid %d and the agent runs as uid %d, which cannot reach that user's systemd manager; run the agent as the daemon's user, or clear resources.dind_shared_limit on the pool",
			errSharedLimitUnsupported, owner, euid)
	}
	name, err := lookup(strconv.Itoa(owner))
	if err != nil {
		return nil, fmt.Errorf("%w: the daemon belongs to uid %d, whose name is needed to reach its systemd manager: %w", errSharedLimitUnsupported, owner, err)
	}
	return systemctlSlices{args: []string{"--user", "--machine=" + name + "@.host"}, run: run}, nil
}

func lookupUsername(uid string) (string, error) {
	u, err := user.LookupId(uid)
	if err != nil {
		return "", err
	}
	return u.Username, nil
}

// defaultSlices finds the slice manager for this backend's daemon.
func (b *DockerBackend) defaultSlices(ctx context.Context) (sliceLimiter, error) {
	info, err := b.api.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("backend: reading the daemon's cgroup driver for a shared docker-in-docker limit: %w", err)
	}
	return systemdSlicesFor(info, b.api.SocketPath(), os.Geteuid(), os.Getenv, lookupUsername, execCommand)
}

// limitSharedSlice sets up the slice a shared-limit pair is created under and
// returns its name. Nothing is created when it fails: a pool that is charged
// for its pair once must not run the pair unbounded, so the create is refused
// with the reason rather than carried on with two whole limits.
func (b *DockerBackend) limitSharedSlice(ctx context.Context, spec Spec) (string, error) {
	lim, err := b.sliceManager(ctx)
	if err != nil {
		return "", err
	}
	unit := sharedSliceName(spec)
	limits := sharedLimits(spec)
	if limits.CPUs <= 0 && limits.MemoryMB <= 0 {
		return "", fmt.Errorf("%w: pool %q asks for a shared limit but its runner carries no CPU or memory limit to share; set resources.cpus or resources.memory_mb, or clear resources.dind_shared_limit",
			errSharedLimitUnsupported, spec.PoolName)
	}
	if err := lim.Limit(ctx, unit, limits); err != nil {
		return "", err
	}
	b.log.Info("docker-in-docker pair held to a shared limit", "runner", spec.Name, "slice", unit,
		"cpus", limits.CPUs, "memory_mb", limits.MemoryMB)
	return unit, nil
}

// releaseSharedSlice lets a pair's slice go, logging rather than failing: the
// containers are gone, and an inactive slice holds nothing.
func (b *DockerBackend) releaseSharedSlice(ctx context.Context, unit string) {
	if unit == "" {
		return
	}
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	lim, err := b.sliceManager(rctx)
	if err == nil {
		err = lim.Release(rctx, unit)
	}
	if err != nil {
		b.log.Warn("could not release a docker-in-docker pair's shared-limit slice; it holds no containers and is gone at the next boot", "slice", unit, "error", err)
	}
}

// sliceManager is the test's limiter when one is set, else the daemon's.
func (b *DockerBackend) sliceManager(ctx context.Context) (sliceLimiter, error) {
	if b.sharedSlices != nil {
		return b.sharedSlices, nil
	}
	return b.defaultSlices(ctx)
}
