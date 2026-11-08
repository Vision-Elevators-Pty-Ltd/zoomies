package backend

import "testing"

// The working set is the usage less every reclaimable page of file cache, with
// what cannot be dropped without I/O put back, and shmem (/dev/shm, tmpfs)
// left in: it is on the anon LRU, not a file list.
func TestWorkingSetDropsReclaimableCacheAndKeepsShmem(t *testing.T) {
	const mb = 1 << 20
	// The idle runner measured on ci-nl-02 (8 Nov): 186 MB used, 66 MB its own,
	// 17 MB of /dev/shm, 90 MB active and 15 MB inactive file cache.
	stats := map[string]uint64{
		"anon": 66 * mb, "shmem": 17 * mb, "file": 117 * mb,
		"active_file": 90 * mb, "inactive_file": 15 * mb, "slab_reclaimable": 1 * mb,
	}
	usage := uint64(186 * mb)
	if got := WorkingSetOf(usage, stats); got != 80*mb {
		t.Fatalf("working set = %d MB, want 186 - 90 - 15 - 1 = 80", got/mb)
	}
	docker := int64(usage - stats["inactive_file"])
	if got := WorkingSetOf(usage, stats); got >= docker {
		t.Fatalf("working set %d MB is no less than Docker's %d MB", got/mb, docker/mb)
	}
	// Dirty and writeback file pages stay: they cannot be dropped without I/O.
	stats["file_dirty"], stats["file_writeback"] = 4*mb, 2*mb
	if got := WorkingSetOf(usage, stats); got != 86*mb {
		t.Fatalf("with 6 MB dirty the working set = %d MB, want 86", got/mb)
	}
	// Never more than the usage, never negative.
	if got := WorkingSetOf(10*mb, map[string]uint64{"active_file": 0, "inactive_file": 0, "file_dirty": 50 * mb}); got != 10*mb {
		t.Fatalf("working set above usage: %d MB", got/mb)
	}
	if got := WorkingSetOf(10*mb, map[string]uint64{"active_file": 20 * mb, "inactive_file": 0}); got != 0 {
		t.Fatalf("negative working set not floored: %d", got)
	}
	// cgroup v1 or an old engine: no file lists, unknown.
	if got := WorkingSetOf(usage, map[string]uint64{"total_inactive_file": 15 * mb, "cache": 100 * mb}); got != 0 {
		t.Fatalf("without the v2 file lists the working set = %d, want 0 (unknown)", got)
	}
}

// A stats sample carries it, and a pair's is the sum of both halves, or
// unknown if either half's is.
func TestASampleAndAPairCarryTheWorkingSet(t *testing.T) {
	var raw statsJSON
	raw.MemoryStats.Usage = 1000 << 20
	raw.MemoryStats.Stats = map[string]uint64{"inactive_file": 100 << 20, "active_file": 300 << 20}
	s := raw.sample()
	if s.MemoryBytes != 900<<20 || s.WorkingSetBytes != 600<<20 {
		t.Fatalf("sample = %d / %d MB, want Docker's 900 and a 600 MB working set", s.MemoryBytes>>20, s.WorkingSetBytes>>20)
	}
	pair := addStats(Stats{WorkingSetBytes: 600 << 20}, Stats{WorkingSetBytes: 2000 << 20})
	if pair.WorkingSetBytes != 2600<<20 {
		t.Fatalf("pair working set = %d MB, want 2600", pair.WorkingSetBytes>>20)
	}
	if half := addStats(Stats{WorkingSetBytes: 600 << 20}, Stats{}); half.WorkingSetBytes != 0 {
		t.Fatalf("a pair with one half unknown has working set %d, want 0 (unknown)", half.WorkingSetBytes)
	}
}
