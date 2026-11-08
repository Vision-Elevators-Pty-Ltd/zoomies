#!/bin/sh
# kvm-smoke.sh: prove that a user's rootless Docker can, or cannot, give a
# runner /dev/kvm, by opening it inside the real runner image.
#
#   kvm-smoke.sh runner-emu <runner image>               must open /dev/kvm
#   kvm-smoke.sh runner-pr  <runner image> --expect-deny must not
#
# Run as root on the host. It reads the user's daemon's gid map
# (rootlesskit's child_pid), translates the host kvm gid through it as the
# agent does, then runs, as that user and through that user's daemon only:
#
#   docker run --rm --device /dev/kvm [--group-add <inside gid>]
#     --entrypoint /bin/sh <image> -c '<probe>'
#
# --entrypoint is required: the runner image's entrypoint.sh ignores a
# command, wants runner credentials and exits 64 without them, so a probe
# passed as a command never runs. The image's own user (runner) is kept. The
# probe prints PROBE-RAN, the process's groups and the device node, opens
# /dev/kvm read-write and asks KVM_GET_API_VERSION, which must be 12.
#
# With --expect-deny, the host kvm gid must be absent from the daemon's map
# (so no group is added), the probe must run (PROBE-RAN) and the open must be
# refused with EACCES. A container that does not start, or a probe that never
# runs, fails the test rather than counting as a denial.
#
# RUN_USER_DIR (default /run/user), PROC (default /proc) and KVM_GID override
# the host's, for tests.
set -eu

[ $# -ge 2 ] || { echo "usage: kvm-smoke.sh USER IMAGE [--expect-deny]" >&2; exit 2; }
user=$1 image=$2 mode=${3:-}
die() { echo "kvm-smoke: FAIL: $*" >&2; exit 1; }

uid=$(id -u "$user") || die "no user $user"
rundir=${RUN_USER_DIR:-/run/user}/$uid
sock=unix://$rundir/docker.sock
kvm_gid=${KVM_GID:-$(getent group kvm | cut -d: -f3)}
[ -n "$kvm_gid" ] || die "there is no kvm group"
pid=$(cat "$rundir/dockerd-rootless/child_pid") || die "$user's rootless docker is not running (no child_pid)"
proc=${PROC:-/proc}
map=$(cat "$proc/$pid/gid_map") || die "cannot read the daemon's gid map"
inside=$(echo "$map" | awk -v g="$kvm_gid" '$2 <= g && g < $2 + $3 { print $1 + g - $2; exit }')

echo "kvm-smoke: $user's daemon (pid $pid) gid map:"
echo "$map" | sed 's/^/  /'

probe='echo PROBE-RAN; id; ls -ln /dev/kvm; exec python3 -c "
import errno, fcntl, os, sys
try:
    fd = os.open(\"/dev/kvm\", os.O_RDWR)
except OSError as e:
    print(\"KVM-OPEN-ERRNO\", errno.errorcode.get(e.errno, e.errno)); sys.exit(13)
print(\"KVM-API\", fcntl.ioctl(fd, 0xAE00))
"'

run() {
	runuser -u "$user" "--" env DOCKER_HOST="$sock" XDG_RUNTIME_DIR="$rundir" \
		docker run --rm --device /dev/kvm "$@" --entrypoint /bin/sh "$image" -c "$probe" 2>&1 || true
}

if [ "$mode" = --expect-deny ]; then
	[ -z "$inside" ] || die "host gid $kvm_gid (kvm) is mapped into $user's daemon as $inside: this daemon can give runners /dev/kvm"
	out=$(run)
	echo "$out" | sed 's/^/  /'
	echo "$out" | grep -q '^PROBE-RAN' || die "the probe never ran, so this proves nothing about the device"
	echo "$out" | grep -q '^KVM-OPEN-ERRNO EACCES' || die "the open was not refused with EACCES"
	echo "kvm-smoke: PASS: $user's runners cannot open /dev/kvm"
	exit 0
fi

[ -n "$inside" ] || die "host gid $kvm_gid (kvm) is not mapped into $user's daemon; check /etc/subgid and restart its docker"
setgroups=$(cat "$proc/$pid/setgroups") || die "cannot read setgroups for $user's daemon"
[ "$setgroups" = allow ] || die "setgroups is $setgroups in $user's daemon namespace, so runc would drop the group"
out=$(run --group-add "$inside")
echo "$out" | sed 's/^/  /'
echo "$out" | grep -q '^PROBE-RAN' || die "the probe never ran (is --entrypoint honoured, and the image right?)"
echo "$out" | grep -q "groups=.*$inside" || die "the runner process was not given group $inside"
echo "$out" | grep -qx 'KVM-API 12' || die "the runner could not open /dev/kvm and get KVM_GET_API_VERSION 12"
echo "kvm-smoke: PASS: $user's runners open /dev/kvm as group $inside (KVM API 12)"
