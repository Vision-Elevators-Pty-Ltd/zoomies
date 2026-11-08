package backend

import "testing"

const wsMiB = 1 << 20

// v2Stats is a cgroup v2 memory.stat map with the keys a Docker engine reports
// for it (rootless, kernel 6.8, read on ci-nl-02 on 8 Nov 2026), all zero but
// those given.
func v2Stats(set map[string]uint64) map[string]uint64 {
	out := map[string]uint64{}
	for _, k := range []string{"active_anon", "active_file", "anon", "anon_thp", "file", "file_dirty", "file_mapped",
		"file_writeback", "inactive_anon", "inactive_file", "kernel_stack", "pgactivate", "pgdeactivate", "pgfault",
		"pglazyfree", "pglazyfreed", "pgmajfault", "pgrefill", "pgscan", "pgsteal", "shmem", "slab",
		"slab_reclaimable", "slab_unreclaimable", "sock", "thp_collapse_alloc", "thp_fault_alloc", "unevictable",
		"workingset_activate", "workingset_nodereclaim", "workingset_refault"} {
		out[k] = 0
	}
	for k, v := range set {
		out[k] = v
	}
	return out
}

// v1Stats is a cgroup v1 memory.stat map with the keys a Docker engine reports
// for it: the file lists under the same names as v2, dirty pages under "dirty"
// and "writeback", and hierarchical totals under total_*.
func v1Stats(set map[string]uint64) map[string]uint64 {
	out := map[string]uint64{}
	for _, k := range []string{"active_anon", "active_file", "cache", "dirty", "hierarchical_memory_limit",
		"hierarchical_memsw_limit", "inactive_anon", "inactive_file", "mapped_file", "pgfault", "pgmajfault",
		"pgpgin", "pgpgout", "rss", "rss_huge", "shmem", "swap", "unevictable", "writeback",
		"total_active_anon", "total_active_file", "total_cache", "total_dirty", "total_inactive_anon",
		"total_inactive_file", "total_mapped_file", "total_pgfault", "total_pgmajfault", "total_pgpgin",
		"total_pgpgout", "total_rss", "total_rss_huge", "total_shmem", "total_swap", "total_unevictable",
		"total_writeback"} {
		out[k] = 0
	}
	for k, v := range set {
		out[k] = v
	}
	return out
}

// The working set is the usage less every reclaimable page of file cache, with
// what cannot be dropped without I/O put back, and shmem (/dev/shm, tmpfs)
// left in: it is on the anon LRU, not a file list.
func TestWorkingSetDropsReclaimableCacheAndKeepsShmem(t *testing.T) {
	// The idle runner measured on ci-nl-02 (8 Nov): 186 MB used, 66 MB its own,
	// 17 MB of /dev/shm, 90 MB active and 15 MB inactive file cache.
	stats := v2Stats(map[string]uint64{
		"anon": 66 * wsMiB, "shmem": 17 * wsMiB, "file": 117 * wsMiB,
		"active_file": 90 * wsMiB, "inactive_file": 15 * wsMiB, "slab_reclaimable": 1 * wsMiB,
	})
	usage := uint64(186 * wsMiB)
	if got := WorkingSetOf(usage, stats); got != 80*wsMiB {
		t.Fatalf("working set = %d MB, want 186 - 90 - 15 - 1 = 80", got/wsMiB)
	}
	if docker := int64(usage - stats["inactive_file"]); WorkingSetOf(usage, stats) >= docker {
		t.Fatalf("working set is no less than Docker's %d MB", docker/wsMiB)
	}
	stats["file_dirty"], stats["file_writeback"] = 4*wsMiB, 2*wsMiB
	if got := WorkingSetOf(usage, stats); got != 86*wsMiB {
		t.Fatalf("with 6 MB dirty the working set = %d MB, want 86", got/wsMiB)
	}
	if got := WorkingSetOf(10*wsMiB, v2Stats(map[string]uint64{"file_dirty": 50 * wsMiB})); got != 10*wsMiB {
		t.Fatalf("working set above usage: %d MB", got/wsMiB)
	}
	if got := WorkingSetOf(10*wsMiB, v2Stats(map[string]uint64{"active_file": 20 * wsMiB})); got != 0 {
		t.Fatalf("negative working set not floored: %d", got)
	}
}

// Review of #20, should-fix: a cgroup v1 map has active_file and inactive_file
// too, so it was read as v2 and its dirty pages, under "dirty", were never put
// back: 1,000 MB used with 500 MB dirty came out a known 300 MB. Anything not
// positively v2 is unknown.
func TestACgroupV1MapIsUnknown(t *testing.T) {
	// The review's probe.
	probe := map[string]uint64{"active_file": 600 * wsMiB, "inactive_file": 100 * wsMiB, "total_active_file": 600 * wsMiB,
		"total_inactive_file": 100 * wsMiB, "dirty": 500 * wsMiB, "writeback": 0, "total_dirty": 500 * wsMiB}
	if got := WorkingSetOf(1000*wsMiB, probe); got != 0 {
		t.Fatalf("the review's v1 map gave a known %d MB, want 0 (unknown)", got/wsMiB)
	}
	v1 := v1Stats(map[string]uint64{"rss": 300 * wsMiB, "cache": 700 * wsMiB, "active_file": 600 * wsMiB,
		"inactive_file": 100 * wsMiB, "dirty": 500 * wsMiB, "total_rss": 300 * wsMiB, "total_cache": 700 * wsMiB})
	if got := WorkingSetOf(1000*wsMiB, v1); got != 0 {
		t.Fatalf("Docker's v1 keys gave a known %d MB, want 0 (unknown)", got/wsMiB)
	}
	// One v2 key missing, or one total_* key present, is not v2.
	partial := v2Stats(nil)
	delete(partial, "file_dirty")
	if got := WorkingSetOf(1000*wsMiB, partial); got != 0 {
		t.Fatalf("a map without file_dirty gave %d MB, want unknown", got/wsMiB)
	}
	mixed := v2Stats(map[string]uint64{"total_cache": 1})
	if got := WorkingSetOf(1000*wsMiB, mixed); got != 0 {
		t.Fatalf("a map with a total_* key gave %d MB, want unknown", got/wsMiB)
	}
}

// A stats sample carries it, and a pair's is the sum of both halves, or
// unknown if either half's is.
func TestASampleAndAPairCarryTheWorkingSet(t *testing.T) {
	var raw statsJSON
	raw.MemoryStats.Usage = 1000 * wsMiB
	raw.MemoryStats.Stats = v2Stats(map[string]uint64{"inactive_file": 100 * wsMiB, "active_file": 300 * wsMiB})
	s := raw.sample()
	if s.MemoryBytes != 900*wsMiB || s.WorkingSetBytes != 600*wsMiB {
		t.Fatalf("sample = %d / %d MB, want Docker's 900 and a 600 MB working set", s.MemoryBytes/wsMiB, s.WorkingSetBytes/wsMiB)
	}
	if pair := addStats(Stats{WorkingSetBytes: 600 * wsMiB}, Stats{WorkingSetBytes: 2000 * wsMiB}); pair.WorkingSetBytes != 2600*wsMiB {
		t.Fatalf("pair working set = %d MB, want 2600", pair.WorkingSetBytes/wsMiB)
	}
	if half := addStats(Stats{WorkingSetBytes: 600 * wsMiB}, Stats{}); half.WorkingSetBytes != 0 {
		t.Fatalf("a pair with one half unknown has working set %d, want 0 (unknown)", half.WorkingSetBytes)
	}
}
