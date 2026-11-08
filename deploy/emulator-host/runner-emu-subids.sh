#!/bin/sh
# runner-emu-subids.sh: create the runner-emu account and give it, and only
# it, the host's kvm group inside its rootless Docker's user namespace.
#
# Run as root on a KVM host (ci-nl-01, ci-nl-02), before that user's rootless
# Docker is installed. Safe to run again: it changes nothing that is already
# right, and refuses, changing nothing, where the files are not as expected.
#
#   1. Create runner-emu if it does not exist. useradd may allocate a
#      subordinate uid and gid range by itself (SUB_UID_COUNT in
#      /etc/login.defs); whatever it allocated is kept and reused, never
#      appended to.
#   2. Where it allocated none, allocate one explicit 65536 range, above every
#      range already in either file.
#   3. Grant the kvm group's gid, alone (usermod --add-subgids GID-GID),
#      after the range, so it is mapped after it in the namespace.
#   4. Re-read both files and check them: runner-emu has exactly one 65536
#      range in each, the kvm grant once in /etc/subgid, no range of
#      runner-emu overlaps any other entry, and no other user holds the kvm
#      gid. Any failure exits non-zero.
#
# The inside gid the kvm group gets is read from the running daemon's
# gid_map afterwards (kvm-smoke.sh), never assumed.
#
# SUBUID, SUBGID and EMU_USER override the defaults, for the tests.
set -eu

EMU_USER=${EMU_USER:-runner-emu}
SUBUID=${SUBUID:-/etc/subuid}
SUBGID=${SUBGID:-/etc/subgid}
RANGE=65536

die() { echo "runner-emu-subids: $*" >&2; exit 1; }

kvm_gid=$(getent group kvm | cut -d: -f3)
[ -n "$kvm_gid" ] || die "there is no kvm group on this host"
[ -c /dev/kvm ] || [ "${SKIP_DEV_CHECK:-}" = 1 ] || die "/dev/kvm is not a character device here"

if ! id -u "$EMU_USER" >/dev/null 2>&1; then
	useradd --create-home --shell /bin/bash "$EMU_USER"
	passwd -l "$EMU_USER" >/dev/null
fi
touch "$SUBUID" "$SUBGID"

# ranges FILE USER: "start count" for every entry of USER that is not the kvm grant.
ranges() { awk -F: -v u="$2" -v g="$kvm_gid" '$1 == u && !($2 == g && $3 == 1) { print $2, $3 }' "$1"; }

if [ -z "$(ranges "$SUBUID" "$EMU_USER")" ] && [ -z "$(ranges "$SUBGID" "$EMU_USER")" ]; then
	# Above every range of every user in both files, and never below 100000.
	start=$(awk -F: 'NF >= 3 { e = $2 + $3; if (e > m) m = e } END { if (m < 100000) m = 100000; print m }' "$SUBUID" "$SUBGID")
	usermod --add-subuids "$start-$((start + RANGE - 1))" --add-subgids "$start-$((start + RANGE - 1))" "$EMU_USER"
fi
if ! grep -qx "$EMU_USER:$kvm_gid:1" "$SUBGID"; then
	usermod --add-subgids "$kvm_gid-$kvm_gid" "$EMU_USER"
fi

# check FILE KIND: runner-emu has exactly one RANGE-long range, and none of its
# ranges overlaps any other entry in the file.
check() {
	awk -F: -v u="$EMU_USER" -v g="$kvm_gid" -v n="$RANGE" -v f="$1" -v kind="$2" '
		NF < 3 { next }
		{ user[NR] = $1; s[NR] = $2 + 0; c[NR] = $3 + 0; rows = NR }
		END {
			mine = 0; grants = 0
			for (i = 1; i <= rows; i++) {
				if (!(i in s)) continue
				if (user[i] == u && s[i] == g && c[i] == 1) { grants++; continue }
				if (s[i] == g && c[i] == 1) { printf "%s: %s also holds the kvm gid %d\n", f, user[i], g; bad = 1 }
				if (user[i] == u) {
					mine++
					if (c[i] != n) { printf "%s: %s has a range of %d, not %d\n", f, u, c[i], n; bad = 1 }
				}
			}
			if (mine != 1) { printf "%s: %s has %d ranges, want exactly 1\n", f, u, mine; bad = 1 }
			if (kind == "gid" && grants != 1) { printf "%s: the kvm grant %s:%d:1 appears %d times, want 1\n", f, u, g, grants; bad = 1 }
			if (kind == "uid" && grants != 0) { printf "%s: %s has the kvm gid as a uid range\n", f, u; bad = 1 }
			for (i = 1; i <= rows; i++) for (j = i + 1; j <= rows; j++) {
				if (!(i in s) || !(j in s)) continue
				if (user[i] != u && user[j] != u) continue
				if (s[i] < s[j] + c[j] && s[j] < s[i] + c[i]) {
					printf "%s: %s:%d:%d overlaps %s:%d:%d\n", f, user[i], s[i], c[i], user[j], s[j], c[j]; bad = 1
				}
			}
			exit bad
		}' "$1"
}
check "$SUBUID" uid || die "$SUBUID is not as it must be; fix it by hand before installing $EMU_USER's docker"
check "$SUBGID" gid || die "$SUBGID is not as it must be; fix it by hand before installing $EMU_USER's docker"

echo "runner-emu-subids: $EMU_USER ready:"
grep "^$EMU_USER:" "$SUBUID" | sed 's/^/  subuid /'
grep "^$EMU_USER:" "$SUBGID" | sed 's/^/  subgid /'
