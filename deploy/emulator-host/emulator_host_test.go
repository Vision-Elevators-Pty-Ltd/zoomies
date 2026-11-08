// Package emulatorhost holds the tests of the host files for the runner-emu
// agent on the KVM hosts: the subordinate-id script, the KVM smoke test and
// the agent's unit (docs/hosts-and-pools.md, Host devices).
package emulatorhost

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func needSh(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("POSIX shell scripts")
	}
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("no sh")
	}
}

// fakeBin writes executable shell scripts into a new directory and returns it,
// for the front of PATH.
func fakeBin(t *testing.T, scripts map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range scripts {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"+body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func run(t *testing.T, env []string, script string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command("sh", append([]string{script}, args...)...)
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// Review r2 of #23, finding 5: useradd may allocate a subordinate range by
// itself before the recipe appended its own, leaving runner-emu with two
// ranges, the second overlapping another user's. The script keeps whatever
// useradd allocated, adds only the kvm gid, allocates a range itself only
// where useradd did not, and refuses files it cannot vouch for.
func TestTheSubidScriptReusesUseraddsRangeAndRefusesOverlaps(t *testing.T) {
	needSh(t)
	for _, tc := range []struct {
		name      string
		autoAlloc bool // useradd allocates 231072:65536, as on Ubuntu
		wantRange string
	}{
		{"useradd allocates a range", true, "runner-emu:231072:65536"},
		{"useradd allocates none", false, "runner-emu:231072:65536"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			subuid, subgid := filepath.Join(dir, "subuid"), filepath.Join(dir, "subgid")
			existing := "runner:100000:65536\nrunner-pr:165536:65536\n"
			for _, f := range []string{subuid, subgid} {
				if err := os.WriteFile(f, []byte(existing), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			alloc := ""
			if tc.autoAlloc {
				alloc = `echo "$u:231072:65536" >> "$SUBUID"; echo "$u:231072:65536" >> "$SUBGID"`
			}
			created := filepath.Join(dir, "created")
			bin := fakeBin(t, map[string]string{
				"getent": `echo kvm:x:994:`,
				"id":     `[ -e "` + created + `" ] && echo 1003`,
				"passwd": `:`,
				"useradd": `for a; do u=$a; done; touch "` + created + `"
` + alloc,
				// usermod --add-subuids A-B --add-subgids A-B USER
				"usermod": `u=; while [ $# -gt 0 ]; do case $1 in
  --add-subuids) r=$2; shift 2; uids="$uids $r";;
  --add-subgids) r=$2; shift 2; gids="$gids $r";;
  *) u=$1; shift;; esac; done
for r in $uids; do echo "$u:${r%-*}:$(( ${r#*-} - ${r%-*} + 1 ))" >> "$SUBUID"; done
for r in $gids; do echo "$u:${r%-*}:$(( ${r#*-} - ${r%-*} + 1 ))" >> "$SUBGID"; done`,
			})
			env := []string{"PATH=" + bin + ":" + os.Getenv("PATH"), "SUBUID=" + subuid, "SUBGID=" + subgid, "SKIP_DEV_CHECK=1"}
			out, err := run(t, env, "runner-emu-subids.sh")
			if err != nil {
				t.Fatalf("the script failed: %v\n%s", err, out)
			}
			uid, _ := os.ReadFile(subuid)
			gid, _ := os.ReadFile(subgid)
			if want := existing + tc.wantRange + "\n"; string(uid) != want {
				t.Fatalf("subuid:\n%s\nwant:\n%s", uid, want)
			}
			if want := existing + tc.wantRange + "\nrunner-emu:994:1\n"; string(gid) != want {
				t.Fatalf("subgid:\n%s\nwant:\n%s", gid, want)
			}
			// Again: nothing changes.
			if out, err := run(t, env, "runner-emu-subids.sh"); err != nil {
				t.Fatalf("a second run failed: %v\n%s", err, out)
			}
			if again, _ := os.ReadFile(subgid); string(again) != string(gid) {
				t.Fatalf("a second run changed subgid:\n%s", again)
			}
			// The r1 recipe's result: a second range appended after useradd's.
			if err := os.WriteFile(subgid, append(gid, []byte("runner-emu:165536:65536\n")...), 0o644); err != nil {
				t.Fatal(err)
			}
			out, err = run(t, env, "runner-emu-subids.sh")
			if err == nil || !strings.Contains(out, "has 2 ranges") || !strings.Contains(out, "overlaps") {
				t.Fatalf("an overlapping second range was accepted: %v\n%s", err, out)
			}
		})
	}
}

// Review r2 of #23, finding 6: the runner image's entrypoint ignores a command
// and exits 64 without runner credentials, so a probe passed as a command
// never ran. The smoke test overrides the entrypoint, adds the gid the
// daemon's live map gives host gid 994, and passes only on PROBE-RAN, that
// group and KVM_GET_API_VERSION 12. Its negative control must see the probe
// run and the open refused with EACCES, on a daemon whose map lacks 994.
func TestTheSmokeTestRunsItsProbeInTheRunnerImage(t *testing.T) {
	needSh(t)
	setup := func(t *testing.T, gidMap, dockerOut string) ([]string, string) {
		dir := t.TempDir()
		rundir := filepath.Join(dir, "run", "1003", "dockerd-rootless")
		proc := filepath.Join(dir, "proc", "4242")
		for _, d := range []string{rundir, proc} {
			if err := os.MkdirAll(d, 0o755); err != nil {
				t.Fatal(err)
			}
		}
		write := func(p, s string) {
			if err := os.WriteFile(p, []byte(s), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		write(filepath.Join(rundir, "child_pid"), "4242\n")
		write(filepath.Join(proc, "gid_map"), gidMap)
		write(filepath.Join(proc, "setgroups"), "allow\n")
		argsFile := filepath.Join(dir, "docker-args")
		bin := fakeBin(t, map[string]string{
			"id":      `echo 1003`,
			"getent":  `echo kvm:x:994:`,
			"runuser": `while [ "$1" != "--" ]; do shift; done; shift; exec "$@"`,
			// The image's real behaviour: without --entrypoint its
			// entrypoint runs and exits 64 whatever the command.
			"docker": `printf '%s\n' "$@" > "` + argsFile + `"
case " $* " in *" --entrypoint /bin/sh "*) ;; *) echo "no credentials supplied."; exit 64;; esac
printf '%b' "` + dockerOut + `"`,
		})
		env := []string{"PATH=" + bin + ":" + os.Getenv("PATH"), "RUN_USER_DIR=" + filepath.Join(dir, "run"), "PROC=" + filepath.Join(dir, "proc")}
		return env, argsFile
	}
	const emuMap = "         0       1003          1\n         1     231072      65536\n     65537        994          1\n"
	const prMap = "         0       1002          1\n         1     165536      65536\n"
	const opened = `PROBE-RAN\nuid=1001(runner) gid=1001(runner) groups=1001(runner),65537\ncrw-rw---- 1 65534 65537 10, 232 /dev/kvm\nKVM-API 12\n`

	env, argsFile := setup(t, emuMap, opened)
	out, err := run(t, env, "kvm-smoke.sh", "runner-emu", "ghcr.io/eyupio/zoomies-runner:latest")
	if err != nil || !strings.Contains(out, "PASS") {
		t.Fatalf("the smoke test failed on a daemon that opens /dev/kvm: %v\n%s", err, out)
	}
	args, _ := os.ReadFile(argsFile)
	for _, want := range []string{"--device\n/dev/kvm\n", "--group-add\n65537\n", "--entrypoint\n/bin/sh\n", "ghcr.io/eyupio/zoomies-runner:latest\n-c\n"} {
		if !strings.Contains(string(args), want) {
			t.Fatalf("docker run lacks %q:\n%s", want, args)
		}
	}

	for name, dockerOut := range map[string]string{
		"the ioctl did not answer 12": `PROBE-RAN\ngroups=1001(runner),65537\nKVM-OPEN-ERRNO EACCES\n`,
		"the probe never ran":         `container failed to start\n`,
		"the group was not given":     `PROBE-RAN\ngroups=1001(runner)\nKVM-API 12\n`,
	} {
		env, _ := setup(t, emuMap, dockerOut)
		if out, err := run(t, env, "kvm-smoke.sh", "runner-emu", "img"); err == nil {
			t.Fatalf("%s: the smoke test passed:\n%s", name, out)
		}
	}
	env, _ = setup(t, prMap, opened)
	if out, err := run(t, env, "kvm-smoke.sh", "runner-emu", "img"); err == nil || !strings.Contains(out, "not mapped") {
		t.Fatalf("a daemon without 994 mapped passed: %v\n%s", err, out)
	}

	// The negative control on an ordinary daemon.
	env, argsFile = setup(t, prMap, `PROBE-RAN\ngroups=1001(runner)\nKVM-OPEN-ERRNO EACCES\n`)
	if out, err := run(t, env, "kvm-smoke.sh", "runner-pr", "img", "--expect-deny"); err != nil || !strings.Contains(out, "PASS") {
		t.Fatalf("the negative control failed: %v\n%s", err, out)
	}
	if args, _ := os.ReadFile(argsFile); strings.Contains(string(args), "--group-add") {
		t.Fatalf("the negative control added a group the ordinary daemon has not mapped:\n%s", args)
	}
	env, _ = setup(t, prMap, `container failed to start\n`)
	if out, err := run(t, env, "kvm-smoke.sh", "runner-pr", "img", "--expect-deny"); err == nil {
		t.Fatalf("a container that never ran counted as a denial:\n%s", out)
	}
	env, _ = setup(t, emuMap, `PROBE-RAN\nKVM-OPEN-ERRNO EACCES\n`)
	if out, err := run(t, env, "kvm-smoke.sh", "runner-pr", "img", "--expect-deny"); err == nil {
		t.Fatalf("a daemon with 994 mapped passed the negative control:\n%s", out)
	}
}

// Review r2 of #23, finding 7: copying another agent's unit could carry its
// StateDirectory (which systemd chowns to the new user), its environment file
// or its groups. The emulator agent's unit is complete and shares nothing.
func TestTheEmulatorAgentUnitIsStandalone(t *testing.T) {
	raw, err := os.ReadFile("zoomies-agent-emulator.service")
	if err != nil {
		t.Fatal(err)
	}
	directives := map[string][]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "[") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			t.Fatalf("not a directive: %q", line)
		}
		directives[k] = append(directives[k], v)
	}
	for _, forbidden := range []string{"StateDirectory", "ConfigurationDirectory", "CacheDirectory", "LogsDirectory",
		"RuntimeDirectory", "EnvironmentFile", "Environment", "SupplementaryGroups", "DynamicUser", "BindPaths", "PassEnvironment"} {
		if v, ok := directives[forbidden]; ok {
			t.Errorf("the unit sets %s=%v, which could share or take over another agent's", forbidden, v)
		}
	}
	want := map[string]string{
		"User":             "runner-emu",
		"Group":            "runner-emu",
		"ExecStart":        "/usr/local/bin/zoomies agent --config /etc/zoomies-emu/zoomies.yaml",
		"WorkingDirectory": "/var/lib/zoomies-emu",
		"ReadWritePaths":   "/var/lib/zoomies-emu",
		"ProtectSystem":    "strict",
		"ProtectHome":      "read-only",
		"KillMode":         "process",
	}
	for k, v := range want {
		if got := directives[k]; len(got) != 1 || got[0] != v {
			t.Errorf("%s = %v, want exactly %q", k, got, v)
		}
	}
	for k, vs := range directives {
		for _, v := range vs {
			if strings.Contains(v, "/etc/zoomies/") || strings.Contains(v, "/var/lib/zoomies/") || strings.HasSuffix(v, "/etc/zoomies") || strings.HasSuffix(v, "/var/lib/zoomies") {
				t.Errorf("%s=%s names the other agents' paths", k, v)
			}
		}
	}
}
