package scheduler

import (
	"fmt"
	"slices"

	"github.com/eyupio/zoomies/internal/store"
)

// osLabels and archLabels split store.ImplicitLabels into the two dimensions a
// pool can genuinely contradict. "self-hosted" belongs to neither: every pool
// in Zoomies is self-hosted, so a job asking for it constrains nothing.
var (
	osLabels   = map[string]bool{"linux": true, "windows": true, "macos": true}
	archLabels = map[string]bool{"x64": true, "arm": true, "arm64": true}
)

const (
	// noMatch is the score of a pool that cannot run a job at all. It is
	// negative so that callers can sort scores and reject anything below zero.
	noMatch = -1
	// exactScore is what a pool scores when it advertises precisely the labels
	// the job asked for. Surplus labels count down from here, which keeps every
	// matching score non-negative for any sane label set.
	exactScore = 1 << 16
)

// Matches reports whether a pool advertising poolLabels may run a job whose
// runs-on listed jobLabels.
//
// The rule mirrors what GitHub itself does. Labels every actions/runner binary
// advertises (store.ImplicitLabels) do not constrain pool selection, because
// "runs-on: [self-hosted, linux, x64]" is how nearly every workflow is written
// and it is not a request for a particular pool. The one exception is a pool
// that declares its own os or arch: that is a promise about the machine, so a
// job asking for a different one must not land there.
func Matches(poolLabels, jobLabels []string) bool {
	return matches(store.NormalizeLabels(poolLabels), labelsOf(store.NormalizeLabels(jobLabels)), true)
}

// PoolMatches is Matches for a whole pool, which knows whether its runners
// advertise the implicit labels at all. A pool registered with
// --no-default-labels carries exactly the labels it lists, so a job asking
// for self-hosted or linux must find those among them: GitHub would never
// offer that job to such a runner, and a runner started for it would sit
// idle while the job waited.
//
// A pool that maps host devices (runner_settings.devices) is stricter: it
// takes only a job that asks for every label of its own that is not an
// implicit one (sa-ci-emulator, say), so a job written runs-on: [self-hosted,
// linux, x64] is never placed on the emulator pool even where nothing else
// could run it. GitHub's side of the same boundary is the pool's runner group,
// restricted to the emulator workflows (controller.deviceGroupRefusal).
func PoolMatches(p *store.Pool, jobLabels []string) bool {
	return poolTakes(p, store.NormalizeLabels(p.Labels), labelsOf(store.NormalizeLabels(jobLabels)))
}

// poolTakes is PoolMatches over a pool's normalised labels and a job's
// classified ones: the one question every place that sets a job against a
// pool asks, so that a device pool's rule cannot be skipped by any of them.
func poolTakes(p *store.Pool, labels []string, job jobLabels) bool {
	if p.MapsDevices() && !asksForEveryOwnLabel(labels, job.names) {
		return false
	}
	return matches(labels, job, !p.NoDefaultLabels)
}

// asksForEveryOwnLabel reports whether a job's labels include every label of
// the pool's that is not one every runner advertises, and whether the pool has
// any such label at all: a device pool without one would be asked for by any
// job, and takes none.
func asksForEveryOwnLabel(pool, job []string) bool {
	own := 0
	for _, l := range pool {
		if store.ImplicitLabels[l] {
			continue
		}
		own++
		if !slices.Contains(job, l) {
			return false
		}
	}
	return own > 0
}

// jobLabels is a job's normalised labels with the one question every
// comparison would otherwise put to a map for itself, answered once: which of
// them are the labels every runner advertises. A pass sets each queued job
// against every pool, and the pools that cannot run it turn it away on its first
// label, so that lookup was the commonest thing the matching did.
type jobLabels struct {
	names []string
	// implicit has bit i set when names[i] is in store.ImplicitLabels, for the
	// first 64 labels. A job has a handful, so the word is enough; the rest ask
	// the map, which is slower and still right.
	implicit uint64
}

// labelsOf classifies a job's labels, which the caller has put through
// NormalizeLabels.
func labelsOf(names []string) jobLabels {
	j := jobLabels{names: names}
	for i, l := range names[:min(len(names), 64)] {
		if store.ImplicitLabels[l] {
			j.implicit |= 1 << i
		}
	}
	return j
}

// isImplicit reports whether the job's i-th label is one every runner advertises.
func (j jobLabels) isImplicit(i int) bool {
	if i < 64 {
		return j.implicit&(1<<i) != 0
	}
	return store.ImplicitLabels[j.names[i]]
}

// matches is Matches over a pool's normalised labels and a job's classified
// ones. implicit says whether the pool's runners advertise store.ImplicitLabels.
func matches(pool []string, job jobLabels, implicit bool) bool {
	for i, l := range job.names {
		if implicit && job.isImplicit(i) {
			continue
		}
		if !slices.Contains(pool, l) {
			return false
		}
	}
	return !contradicts(pool, job.names, osLabels) && !contradicts(pool, job.names, archLabels)
}

// contradicts reports whether the job asked for a label from dim that the pool
// lacks while declaring a different label from the same dimension. A pool that
// declares nothing in a dimension makes no promise and contradicts nothing.
func contradicts(pool, job []string, dim map[string]bool) bool {
	declares := slices.ContainsFunc(pool, func(l string) bool { return dim[l] })
	if !declares {
		return false
	}
	return slices.ContainsFunc(job, func(l string) bool {
		return dim[l] && !slices.Contains(pool, l)
	})
}

// Score ranks how well a pool fits a job. Higher is better; a negative score
// means the pool cannot run the job at all.
//
// Every matching pool satisfies all of the job's explicit labels by
// construction, so the only thing left to rank on is surplus: capability the
// job never asked for. Preferring the smallest surplus keeps a specialised
// pool (say gpu + cuda12) free for the jobs that actually need it.
func Score(poolLabels, jobLabels []string) int {
	return score(poolLabels, jobLabels, true)
}

// score is Score for a pool that may not advertise the implicit labels.
func score(poolLabels, jobLabels []string, implicit bool) int {
	pool := store.NormalizeLabels(poolLabels)
	job := labelsOf(store.NormalizeLabels(jobLabels))
	if !matches(pool, job, implicit) {
		return noMatch
	}
	return surplusScore(pool, job.names)
}

// surplusScore is the score of a pool already known to match a job, both
// normalised: how little of what the pool advertises the job never asked for.
func surplusScore(pool, job []string) int {
	surplus := 0
	for _, l := range pool {
		if !slices.Contains(job, l) {
			surplus++
		}
	}
	return max(exactScore-surplus, 0)
}

// Eligible reports whether a pool may run a job at all, and when it may not,
// says why in the operator's words.
//
// The three tests are asked in the order an operator would ask them. A
// disabled pool was disabled on purpose. A pool belonging to another GitHub
// App installation cannot run the job whatever its labels say: Zoomies would
// mint the runner's registration in the wrong GitHub target, and the job would
// sit queued while a runner waited for it somewhere it can never be offered.
// Labels come last, because they are the only one of the three an operator
// changes by editing a workflow.
func Eligible(p *store.Pool, j *store.Job) (bool, string) {
	if p == nil || j == nil {
		return false, ""
	}
	if why := refusalBeforeLabels(p, j); why != "" {
		return false, why
	}
	if !PoolMatches(p, j.Labels) {
		return false, "the pool does not advertise those labels"
	}
	return true, ""
}

// refusalBeforeLabels is the first reason a pool cannot run a job whatever
// either of them is labelled, or "" when the labels are the only question left.
// It is Eligible's first two questions, apart so that a pass can ask them
// without asking the third.
func refusalBeforeLabels(p *store.Pool, j *store.Job) string {
	switch {
	case !p.Enabled:
		return "the pool is disabled"
	case j.InstallationID == "":
		return "no GitHub App installation here covers that repository"
	case p.InstallationID != j.InstallationID:
		return "the pool belongs to another GitHub App installation"
	}
	return ""
}

// BestPool returns the pool that best fits a job, or nil when no pool is
// eligible for it -- which the caller surfaces as a configuration problem
// rather than silently dropping the job.
//
// Ties break on pool name and then pool ID, so the same job always lands in
// the same pool. Anything else would make the controller disagree with itself
// across restarts, and make a scaling decision impossible to explain.
func BestPool(pools []*store.Pool, j *store.Job) *store.Pool {
	best, _ := bestPool(pools, j, nil)
	return best
}

// bestPool is BestPool that also explains an empty answer, given the target
// each installation manages so that the explanation can name one.
//
// The explanation names the nearest miss rather than every pool's objection.
// A job whose labels match a pool in another installation is a different
// problem from one whose labels match nothing here -- the first is a pool on
// the wrong installation, the second is a workflow asking for a machine this
// fleet does not offer -- and only the first is worth a sentence, because the
// second is what the Jobs page already says.
func bestPool(pools []*store.Pool, j *store.Job, targets map[string]string) (*store.Pool, string) {
	return bestOf(pools, nil, j, labelsOf(store.NormalizeLabels(j.Labels)), targets)
}

// preparedLabels is each pool's labels put through NormalizeLabels, in the
// order of pools, for bestOf to compare instead of normalising them again at
// every comparison. A nil pool has none.
func preparedLabels(pools []*store.Pool) [][]string {
	out := make([][]string, len(pools))
	for i, p := range pools {
		if p != nil {
			out[i] = store.NormalizeLabels(p.Labels)
		}
	}
	return out
}

// bestOf is bestPool for a job whose labels the caller has put through
// NormalizeLabels and classified (labelsOf). prepared, from preparedLabels, is
// the pools' labels put through NormalizeLabels too, for a caller that sets many
// jobs against the same pools. Nil makes bestOf normalise each pool's as it
// meets it, which is the better way to ask about one job: labels already in
// order are returned as they are, so nothing is allocated, where preparing them
// would build a slice as long as the fleet for a single comparison.
//
// It gives the answer bestPool always gave, which labels_prepared_test.go holds
// it to over a great many random fleets; what differs is how much it does to
// get there.
//
// A pass sets every queued job against every pool, so whatever one comparison
// repeats is paid jobs times pools over. Each used to normalise both sides to
// ask whether the pool was eligible, and then again, either to score it or to
// see whether it was a near miss. Labels that are already in order come back
// unchanged, but learning that means looking at every one, and that look was
// most of what a pass cost: BenchmarkDecide/burst/100pools, a thousand queued
// jobs against a hundred pools, took about 27 ms, 270 ns a pair, and takes about
// 4 ms now. A pair costs one matches call and, when the pool is eligible, one
// pass over its labels to score it.
func bestOf(pools []*store.Pool, prepared [][]string, j *store.Job, job jobLabels, targets map[string]string) (*store.Pool, string) {
	var best, elsewhere *store.Pool
	bestScore := noMatch
	for i, p := range pools {
		if p == nil || !p.Enabled {
			continue
		}
		var labels []string
		if prepared != nil {
			labels = prepared[i]
		} else {
			labels = store.NormalizeLabels(p.Labels)
		}
		if !poolTakes(p, labels, job) {
			continue
		}
		if refusalBeforeLabels(p, j) != "" {
			// A pool that would take the job but for its installation is the
			// near miss worth reporting. Ties break the same way matches do,
			// so the sentence does not change between passes.
			if elsewhere == nil || lessPool(p, elsewhere) {
				elsewhere = p
			}
			continue
		}
		s := surplusScore(labels, job.names)
		if best == nil || s > bestScore || (s == bestScore && lessPoolFor(j, p, best)) {
			best, bestScore = p, s
		}
	}
	if best != nil || elsewhere == nil {
		return best, ""
	}
	if j.InstallationID == "" {
		return nil, fmt.Sprintf("its labels match pool %s, but no GitHub App installation here covers %s", elsewhere.Name, j.Repo)
	}
	return nil, fmt.Sprintf("its labels match pool %s, which belongs to installation %s",
		elsewhere.Name, installationName(elsewhere.InstallationID, targets))
}

// installationName prefers the target an installation manages -- the
// organisation or repository an operator recognises -- and falls back to the
// identifier when the caller did not supply the targets.
func installationName(id string, targets map[string]string) string {
	if t := targets[id]; t != "" {
		return t
	}
	return id
}

// lessPoolFor is lessPool for a job that has been routed to a size class: of
// two pools that fit it equally, the one in the class it is routed to comes
// first, then a larger class and then a smaller one, nearest first, and among
// the pools of one class the architecture that is the default.
//
// A pool an operator made has no class, and sits between the pool in the right
// class and every other: where it fits a job exactly as well as the pools the
// controller keeps, it was not made for a class and has no reason to lose to
// one made for the wrong one. A job that has not been routed, which is every job
// while size routing is off, is ordered exactly as it always was.
func lessPoolFor(j *store.Job, a, b *store.Pool) bool {
	if !j.RoutedClass.Valid() {
		return lessPool(a, b)
	}
	if ra, rb := classPreference(j.RoutedClass, a), classPreference(j.RoutedClass, b); ra != rb {
		return ra < rb
	}
	if aa, ab := a.FromHosts() && a.Platform.Arch != "amd64", b.FromHosts() && b.Platform.Arch != "amd64"; aa != ab {
		return ab
	}
	return lessPool(a, b)
}

// classPreference ranks a pool for a job routed to want: 0 for the pool in that
// class, 1 for a pool no class was made for, then the larger classes in order
// and then the smaller ones.
func classPreference(want store.SizeClass, p *store.Pool) int {
	if !p.FromHosts() {
		return 1
	}
	_, have, ok := store.ParseAutoKey(p.AutoKey)
	if !ok {
		return 1
	}
	switch d := have.Rank() - want.Rank(); {
	case d == 0:
		return 0
	case d > 0:
		return 1 + d
	default:
		return 10 - d
	}
}

// lessPool is the total order used wherever two pools would otherwise tie.
func lessPool(a, b *store.Pool) bool {
	if a.Name != b.Name {
		return a.Name < b.Name
	}
	return a.ID < b.ID
}
