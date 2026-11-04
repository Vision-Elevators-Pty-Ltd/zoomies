package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/eyupio/zoomies/internal/github"
	"github.com/eyupio/zoomies/internal/store"
)

// rateLimitBackoff is how long to stand down from an installation when GitHub
// says it is out of quota and does not say when that ends. Polling and reaping
// are background paths; spending the last of an installation's quota on them
// would take the webhook path's own API calls -- minting JIT configs -- down
// with it.
const rateLimitBackoff = 15 * time.Minute

const (
	// rateLimitSlack is added to a reset GitHub named. Resuming on the exact
	// second races its own accounting and buys another refusal, which costs
	// the call and starts the wait again.
	rateLimitSlack = 5 * time.Second

	// maxRateLimitBackoff caps a stand-down however far away the reset says it
	// is. GitHub's primary window is an hour, so anything beyond that is a
	// clock out of step or a proxy inventing a header -- and taking an
	// installation out of service for a day on either would be a far worse
	// failure than one more refused call.
	maxRateLimitBackoff = time.Hour
)

// rateLimitHold is when an installation refused for quota may be used again.
//
// GitHub sends the answer on the response, so the fixed backoff is only what
// to do when it did not: waiting a flat fifteen minutes for a quota that came
// back in two wastes the difference on every sweep, and for one that comes
// back in fifty spends the rest of the window discovering that again.
func rateLimitHold(err error, now time.Time) time.Time {
	until, ok := github.RetryAfterRateLimit(err, now)
	if !ok {
		return now.Add(rateLimitBackoff)
	}
	until = until.Add(rateLimitSlack)
	if capped := now.Add(maxRateLimitBackoff); until.After(capped) {
		return capped
	}
	return until
}

// pollLoop is the webhook fallback.
//
// A controller behind NAT, or one whose webhook secret was mistyped, would
// otherwise stop scaling silently, and a fleet that has quietly stopped
// scaling looks exactly like a quiet fleet.
func (c *Controller) pollLoop(ctx context.Context) {
	if !c.cfg().GitHub.PollFallback {
		c.log.Info("the fallback poller is off; scaling depends entirely on webhooks reaching this controller")
		return
	}
	interval := c.pollInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// The poller exists to find work this fleet should run. A fenced
			// fleet is not going to run any, and sweeping would spend the
			// installation's GitHub quota to learn something it cannot act on.
			if !c.Fenced().Fenced {
				c.discoverJobs(ctx)
				c.polls.Add(1)
			}
		case <-c.pollSettingsChanged:
			// github.poll_interval is a runtime setting. The timer was built
			// from the old value, so it is rebuilt here rather than left to
			// fire once more on the old interval.
		}
		if d := c.pollInterval(); d != interval {
			interval = d
			ticker.Reset(d)
		}
	}
}

func (c *Controller) pollInterval() time.Duration {
	if d := c.cfg().GitHub.PollInterval; d > 0 {
		return d
	}
	return 30 * time.Second
}

// pollOnce lists unfinished jobs for every installation and records what it
// finds.
//
// Queue discovery skips installations with recent webhooks. Known unfinished
// jobs are checked separately in bounded pages so a missing completion cannot
// be hidden by unrelated deliveries, including jobs hosted elsewhere.
func (c *Controller) pollOnce(ctx context.Context) {
	c.reconcileKnownJobs(ctx, c.Now())
	c.discoverJobs(ctx)
}

func (c *Controller) discoverJobs(ctx context.Context) {
	now := c.Now()

	// Accepted deliveries only: one that was rejected recorded a job for
	// nobody, and a run of them is the mistyped-secret case this poller is
	// the safety net for.
	//
	// The fleet-wide answer is only used to say whether a webhook has ever
	// arrived, which the Overview reports about the deployment rather than
	// about any one installation. What to poll is decided per installation
	// below.
	last, err := c.st.LastAcceptedDeliveryAt(ctx)
	if err != nil {
		c.log.Error("could not tell when the last webhook arrived", "error", err)
		return
	}
	c.pollingOnly.Store(last.IsZero())

	fresh, err := c.st.InstallationsFreshSince(ctx, now.Add(-2*c.pollInterval()))
	if err != nil {
		c.log.Error("could not tell which installations webhooks are arriving for", "error", err)
		return
	}

	insts, err := c.st.ListInstallations(ctx)
	if err != nil {
		c.log.Error("could not list installations to poll", "error", err)
		return
	}

	found, changed := 0, 0
	// listed is every job this sweep's listings held, queued or running, and
	// swept the installations whose listing came back: a job of one of those
	// that is held here as queued and was listed by none of them is a job
	// GitHub no longer has waiting. See noteUnlistedQueuedJobs.
	listed := map[int64]bool{}
	swept := map[string]bool{}
	for _, inst := range insts {
		if ctx.Err() != nil {
			return
		}
		if IsDemoID(inst.ID) {
			// The demo fixtures are already in the database; there is nothing
			// to poll for, and trying would log a credential failure on every
			// tick of a demo instance.
			continue
		}
		if c.githubHeld(inst.ID, now) {
			continue
		}
		// Skip discovery for this installation when its deliveries are fresh.
		// Known job reconciliation above remains independent of that signal.
		if fresh[inst.ID] {
			continue
		}
		client, err := c.clients.get(ctx, inst)
		if err != nil {
			c.log.Warn("skipping an installation while polling", "installation", inst.ID, "error", err)
			continue
		}
		jobs, err := client.ListQueuedJobs(ctx)
		c.observeGitHub(inst.ID, err)
		if err != nil {
			if errors.Is(err, github.ErrRateLimited) {
				// Hold this installation only. Its quota is its own, and the
				// sweep abandoning the rest would let one organisation out of
				// quota stop every other one from scaling.
				until := rateLimitHold(err, now)
				c.holdGitHub(inst.ID, until)
				c.log.Warn("GitHub rate-limited the fallback poller for an installation; standing down for it",
					"installation", inst.ID, "until", until.UTC().Format(time.RFC3339), "error", err)
				continue
			}
			c.log.Warn("could not poll for queued jobs", "installation", inst.ID, "error", err)
			continue
		}
		// Waiting and running both: "found" is what the listing held, and
		// "changed" the part of it still waiting for a runner.
		found += len(jobs)
		for _, q := range jobs {
			listed[q.ID] = true
		}
		n, err := c.ingestQueuedJobs(ctx, inst, jobs)
		if err != nil {
			c.log.Error("could not record polled jobs", "installation", inst.ID, "error", err)
			continue
		}
		swept[inst.ID] = true
		changed += n
	}
	c.noteUnlistedQueuedJobs(ctx, now, listed, swept)

	if found > 0 {
		c.log.Debug("polled GitHub for unfinished jobs", "found", found, "new_or_changed", changed)
	}
	if changed > 0 {
		c.Nudge()
	}

	// Stamped here, at the end, rather than in the loop that calls this: what
	// an operator wants to know is when the poller last got all the way round,
	// and a sweep that returned early because it could not read its own
	// database did not. The early returns above deliberately leave the stamp
	// where it was, so the interval keeps growing and poller.stale fires.
	c.lastPollAt.Store(c.Now().UnixNano())
}

// ingestQueuedJobs folds polled jobs into the same rows the webhook path
// writes, so both paths feed one scheduler and one history.
//
// A job is attributed to the installation covering its repository, resolved
// the same way the webhook path resolves it, rather than to the installation
// this poll happened to come from. The two are usually the same and are not
// always: an organisation installation and a repository one can both cover a
// repository, and if each path picked its own answer the job's installation
// would flip with every delivery. The polled installation is the fallback,
// because it did report the job.
//
// The count returned is of jobs left waiting, which is what the caller wakes
// the scheduler for. A job found already running is handed to
// ingestStartedJob, which wakes it itself.
func (c *Controller) ingestQueuedJobs(ctx context.Context, polled *store.Installation, jobs []github.QueuedJob) (int, error) {
	return c.ingestQueuedJobsWith(ctx, polled, jobs, newRepoOwners(func(ctx context.Context, repo string) (string, bool) {
		return c.owningInstallation(ctx, repo, polled)
	}))
}

// ingestQueuedJobsWith is ingestQueuedJobs with the answers to which
// installation covers a repository in the caller's hands: they are asked for
// once for each repository a sweep meets, not once for each of its jobs.
func (c *Controller) ingestQueuedJobsWith(ctx context.Context, polled *store.Installation, jobs []github.QueuedJob, owners *repoOwners) (int, error) {
	if len(jobs) == 0 {
		return 0, nil
	}
	// Only what a claim reads of each pool is read, and the pools are used for
	// nothing else here but naming a job's pool in the frame published for it:
	// see ListPoolsForClaim.
	claimable, err := c.st.ListPoolsForClaim(ctx)
	if err != nil {
		return 0, fmt.Errorf("listing pools to match polled jobs: %w", err)
	}

	changed := 0
	for _, q := range jobs {
		if store.JobState(q.Status) == store.JobInProgress {
			c.ingestStartedJob(ctx, q)
			continue
		}
		job := &store.Job{
			GitHubJobID:    q.ID,
			GitHubRunID:    q.RunID,
			RunNumber:      q.RunNumber,
			Repo:           q.Repo,
			Workflow:       q.WorkflowName,
			JobName:        q.JobName,
			Labels:         store.NormalizeLabels(q.Labels),
			State:          store.JobQueued,
			InstallationID: owners.of(ctx, q.Repo),
			RunnerName:     q.RunnerName,
			HTMLURL:        q.HTMLURL,
			QueuedAt:       q.QueuedAt,
		}
		if job.QueuedAt.IsZero() {
			job.QueuedAt = c.Now()
		}
		classing := c.sizeOnArrival(ctx, job, claimable)
		if p := c.bestPool(claimable, job); p != nil {
			job.PoolID = p.ID
			job.Matched = true
		}
		heldToArrival(job, c.Now())
		saved, change, err := c.st.ApplyJob(ctx, job)
		if err != nil {
			c.log.Warn("could not record a polled job", "github_job_id", q.ID, "error", err)
			continue
		}
		saved = c.stampSize(ctx, saved, classing)
		c.recordJobChange(ctx, saved, change, sourcePoller, nil)
		if saved.State == store.JobQueued {
			changed++
		}
		c.publishJobIn(ctx, saved, claimable)
	}
	return changed, nil
}

// ingestStartedJob records a job a sweep found running on one of this fleet's
// runners before anything here had recorded it as started.
//
// Without webhooks that is the ordinary case, not a rare one: an idle runner
// takes a job within a second or two of GitHub queuing it, far inside the
// sweep interval, so the job is never seen queued at all. Nothing else moves a
// runner to busy, and reconcileKnownJobs only rechecks rows that exist, so the
// runner stayed idle on paper for the whole build and stood in for the next
// queued job, which then waited for a runner the pool had room to start.
//
// It goes through applyWorkflowJob because that is what a delivered
// in_progress would have gone through: one owner for the job row, the runner
// link and the busy transition, with the store refusing to move either
// backwards. Two cases are left where they were. A job this controller already
// has as started or finished belongs to reconcileKnownJobs, and applying it
// again on every sweep would cost a write and a scheduling pass per running
// job for nothing. A job running somewhere else that has no row here is left
// alone too: it occupies nothing here, and a row for it would never be
// reconciled to a completion.
//
// A job that does have a row here, still queued, is started whatever runner
// GitHub names, or none. The row is demand until something moves it, and a
// job one of the organisation's other runners took (a static runner, another
// pool's), or one GitHub has not yet named a runner for, is as started as one
// of ours: the scheduler kept starting runners for such jobs until the
// known-job check reached them two minutes or more later, and that check
// applies the same in_progress from GetWorkflowJob that is on this page for
// free.
func (c *Controller) ingestStartedJob(ctx context.Context, q github.QueuedJob) {
	known, err := c.st.GetJobByGitHubID(ctx, q.ID)
	switch {
	case err == nil:
		if known.State == store.JobInProgress || known.State == store.JobCompleted {
			return
		}
	case !errors.Is(err, store.ErrNotFound):
		c.log.Warn("could not look up a polled running job", "github_job_id", q.ID, "error", err)
		return
	default:
		if q.RunnerName == "" {
			return
		}
		if _, err := c.st.GetRunnerByName(ctx, q.RunnerName); err != nil {
			if !errors.Is(err, store.ErrNotFound) {
				c.log.Warn("could not look up the runner of a polled running job",
					"github_job_id", q.ID, "runner", q.RunnerName, "error", err)
			}
			return
		}
	}
	started := &github.WorkflowJobEvent{
		Action:       string(store.JobInProgress),
		JobID:        q.ID,
		RunID:        q.RunID,
		Repo:         q.Repo,
		WorkflowName: q.WorkflowName,
		JobName:      q.JobName,
		Labels:       q.Labels,
		RunnerName:   q.RunnerName,
		Status:       q.Status,
		QueuedAt:     q.QueuedAt,
		HTMLURL:      q.HTMLURL,
	}
	if q.StartedAt != nil {
		started.StartedAt = *q.StartedAt
	}
	if err := c.applyWorkflowJob(ctx, started, sourcePoller, nil); err != nil {
		c.log.Warn("could not record a polled running job", "github_job_id", q.ID, "error", err)
	}
}

// holdRateLimited stands every background sweep down from one installation for
// as long as GitHub asked, and says so once.
func (c *Controller) holdRateLimited(id string, err error, now time.Time, doing string) {
	until := rateLimitHold(err, now)
	c.holdGitHub(id, until)
	c.log.Warn("GitHub rate-limited this installation; standing down from it",
		"installation", id, "while", doing, "until", until.UTC().Format(time.RFC3339), "error", err)
}

// githubHeld reports whether an installation is inside its rate-limit backoff,
// clearing the hold once it has expired so the map does not grow with the
// installations that have long since recovered.
func (c *Controller) githubHeld(id string, now time.Time) bool {
	c.githubMu.Lock()
	defer c.githubMu.Unlock()
	until, ok := c.githubPaused[id]
	if !ok {
		return false
	}
	if !now.Before(until) {
		delete(c.githubPaused, id)
		return false
	}
	return true
}

// heldInstallations lists the installations currently inside a rate-limit
// backoff, with the moment each may be used again.
//
// Expired holds are dropped here as they are in githubHeld, so a caller that
// only reads never leaves the map growing with installations that recovered
// hours ago.
func (c *Controller) heldInstallations(now time.Time) map[string]time.Time {
	c.githubMu.Lock()
	defer c.githubMu.Unlock()
	held := make(map[string]time.Time, len(c.githubPaused))
	for id, until := range c.githubPaused {
		if !now.Before(until) {
			delete(c.githubPaused, id)
			continue
		}
		held[id] = until
	}
	return held
}

// holdGitHub stands the poller down for one installation until a moment.
func (c *Controller) holdGitHub(id string, until time.Time) {
	c.githubMu.Lock()
	defer c.githubMu.Unlock()
	if c.githubPaused == nil {
		c.githubPaused = map[string]time.Time{}
	}
	c.githubPaused[id] = until
}

// owningInstallation is the installation covering a repository, with a
// fallback for the caller that already knows one. A lookup failure is not
// worth abandoning an ingest over: the poller's own installation is a worse
// answer than the repository's and a much better one than none. The answer is
// settled unless the lookup failed: a repository nothing covers is settled, as
// the fallback, and a failed read is not.
func (c *Controller) owningInstallation(ctx context.Context, repo string, fallback *store.Installation) (id string, settled bool) {
	owner, err := c.st.FindInstallationByTarget(ctx, repo)
	switch {
	case err == nil:
		return owner.ID, true
	case errors.Is(err, store.ErrNotFound):
		return installationID(fallback), true
	}
	c.log.Warn("could not resolve the installation covering a polled repository",
		"repo", repo, "error", err)
	return installationID(fallback), false
}

// repoOwners remembers which installation covers each repository for the length
// of one sweep. A sweep meets many jobs and few repositories, and asking the
// store for every job was a query parsed afresh each time: 13% of a sweep over a
// hundred known jobs in a profile. Which installation covers a repository does not
// change in the milliseconds a sweep takes, and the next sweep builds its own, so
// an installation added in between is the next one's answer.
//
// Only an answer is kept. A lookup that failed for a reason other than there
// being no installation says nothing about the repository, so the repository's
// next job asks again, as it did before. Repositories are told apart as they are
// written; the store folds their case, and the cost of not folding it here is one
// more question. It is for one goroutine, which is how a sweep runs.
type repoOwners struct {
	resolve func(ctx context.Context, repo string) (id string, settled bool)
	byRepo  map[string]string
}

func newRepoOwners(resolve func(ctx context.Context, repo string) (id string, settled bool)) *repoOwners {
	return &repoOwners{resolve: resolve, byRepo: map[string]string{}}
}

// of is the installation covering repo, asking resolve only the first time it
// gives an answer worth keeping.
func (o *repoOwners) of(ctx context.Context, repo string) string {
	if id, ok := o.byRepo[repo]; ok {
		return id
	}
	id, settled := o.resolve(ctx, repo)
	if settled {
		o.byRepo[repo] = id
	}
	return id
}

// A healthy stream of unrelated webhooks does not prove every completion was
// delivered. Check a rotating, bounded page of older unfinished jobs even when
// queue discovery stands down. Include work hosted elsewhere: the Jobs page
// still reports it, so restricting recovery to managed jobs leaves those rows
// running for ever when their completion delivery is missed.
// A 404 or API failure is not a cancellation.
//
// Each pass makes at most reconcileBatch GetWorkflowJob calls, and that bound
// is what the pass costs the installation's quota whatever it checks. The jobs
// a sweep found missing from GitHub's queue (startChecks) go first, up to
// startCheckShare of the batch, because each of them is likely demand that is
// no longer there and is starting runners nobody will use; the rest of the
// batch is the rotation over older unfinished jobs, which is never squeezed
// below reconcileBatch-startCheckShare so that missed completions are still
// found at a steady pace.
func (c *Controller) reconcileKnownJobs(ctx context.Context, now time.Time) {
	checks := c.takeStartChecks(ctx, now, startCheckShare)
	checked := make(map[string]bool, len(checks))
	for _, j := range checks {
		checked[j.ID] = true
	}

	cutoff := now.Add(-2 * time.Minute)
	offset := int(c.jobPollOffset.Load())
	jobs, total, err := c.st.ListJobs(ctx, store.JobFilter{
		States: []store.JobState{store.JobQueued, store.JobInProgress}, Until: &cutoff,
	}, store.Page{Limit: reconcileBatch - len(checks), Offset: offset})
	if err != nil {
		c.log.Warn("could not list jobs to reconcile", "error", err)
		jobs = nil
	} else {
		next := offset + len(jobs)
		if next >= total {
			next = 0
		}
		c.jobPollOffset.Store(int64(next))
	}
	for _, j := range jobs {
		if !checked[j.ID] {
			checks = append(checks, j)
			checked[j.ID] = true
		}
	}
	for _, j := range checks {
		if ctx.Err() != nil {
			return
		}
		c.reconcileKnownJob(ctx, j, now)
	}
}

const (
	// reconcileBatch is how many known jobs one job-recovery pass may check
	// against GitHub, one call each. At the 15 s poll interval that is 2,400
	// calls an hour against an installation's 5,000, and it is the whole of
	// what the pass spends: the start checks below come out of it, not on top.
	reconcileBatch = 10
	// startCheckShare is the most of a batch the start checks may take.
	startCheckShare = 7
	// startRecheckAfter is how long a job a start check found still queued
	// waits before another sweep's absence can put it back on the list. The
	// listing is bounded (maxPollRuns, maxPollRequests), so on a busy
	// organisation some genuinely queued jobs are left out of every sweep; this
	// keeps them from taking the share on every pass while the rotation and the
	// jobs that really did start wait.
	startRecheckAfter = time.Minute
)

// reconcileKnownJob checks one known unfinished job against GitHub and applies
// GitHub's start or completion. A job GitHub still has queued, a lookup that
// failed and an installation inside its rate-limit hold all leave the row as
// it is.
func (c *Controller) reconcileKnownJob(ctx context.Context, j *store.Job, now time.Time) {
	if j.InstallationID == "" || IsDemoID(j.InstallationID) || c.githubHeld(j.InstallationID, now) {
		return
	}
	inst, err := c.st.GetInstallation(ctx, j.InstallationID)
	if err != nil {
		return
	}
	client, err := c.clients.get(ctx, inst)
	if err != nil {
		return
	}
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	remote, err := client.GetWorkflowJob(checkCtx, j.Repo, j.GitHubJobID)
	cancel()
	c.observeGitHub(inst.ID, err)
	if err != nil {
		if errors.Is(err, github.ErrRateLimited) {
			c.holdRateLimited(inst.ID, err, now, "reconciling jobs")
		}
		c.log.Warn("could not reconcile workflow job", "job", j.ID, "error", err)
		return
	}
	if remote.State() != store.JobCompleted && remote.State() != store.JobInProgress {
		return
	}
	if err := c.applyWorkflowJob(ctx, remote, sourcePoller, nil); err != nil {
		c.log.Warn("could not update reconciled workflow job", "job", j.ID, "error", err)
	}
}

// noteUnlistedQueuedJobs puts on startChecks every job held here as queued
// that this sweep's listings did not hold, queued or running, for an
// installation whose listing came back.
//
// That is what a started job looks like to a controller without webhooks: an
// idle runner takes a job within seconds, a short job is over before the next
// sweep, and a finished job is in no listing, so the row stays queued, and
// keeps the scheduler starting runners for it, until the known-job rotation
// reaches it, at the earliest two minutes after it was queued. It costs no
// GitHub call here; the check is made by the job-recovery loop, inside its
// own bounded batch.
//
// An installation whose listing failed, was skipped for fresh webhooks or is
// inside a rate-limit hold says nothing about its jobs, so they are left out.
// A job checked within startRecheckAfter is left out too: it was still queued
// on GitHub then, and missing from a bounded listing is no new evidence.
func (c *Controller) noteUnlistedQueuedJobs(ctx context.Context, now time.Time, listed map[int64]bool, swept map[string]bool) {
	if len(swept) == 0 {
		return
	}
	queued, err := c.st.ListQueuedJobs(ctx)
	if err != nil {
		c.log.Warn("could not list queued jobs to compare with the sweep", "error", err)
		return
	}
	c.startChecksMu.Lock()
	defer c.startChecksMu.Unlock()
	for id, at := range c.startChecked {
		if now.Sub(at) >= startRecheckAfter {
			delete(c.startChecked, id)
		}
	}
	for _, j := range queued {
		if listed[j.GitHubJobID] || !swept[j.InstallationID] {
			continue
		}
		if _, recent := c.startChecked[j.ID]; recent {
			continue
		}
		if _, ok := c.startChecks[j.ID]; ok {
			continue
		}
		if c.startChecks == nil {
			c.startChecks = map[string]time.Time{}
		}
		c.startChecks[j.ID] = now
	}
}

// takeStartChecks takes up to n jobs off startChecks for this pass to check,
// the longest-waiting first, and stamps each as checked.
//
// A job that is no longer queued here (a sweep or a delivery has moved it
// since) is dropped without costing a slot or a call. One whose installation
// is inside a rate-limit hold is left on the list for a pass after the hold,
// rather than taking a slot it could not use.
func (c *Controller) takeStartChecks(ctx context.Context, now time.Time, n int) []*store.Job {
	type pending struct {
		id string
		at time.Time
	}
	c.startChecksMu.Lock()
	order := make([]pending, 0, len(c.startChecks))
	for id, at := range c.startChecks {
		order = append(order, pending{id, at})
	}
	c.startChecksMu.Unlock()
	if len(order) == 0 {
		return nil
	}
	slices.SortFunc(order, func(a, b pending) int {
		if d := a.at.Compare(b.at); d != 0 {
			return d
		}
		return strings.Compare(a.id, b.id)
	})

	var out []*store.Job
	for _, p := range order {
		if len(out) >= n || ctx.Err() != nil {
			break
		}
		j, err := c.st.GetJob(ctx, p.id)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			// A failed read says nothing about the job; leave it listed.
			c.log.Warn("could not read a job to check whether it started", "job", p.id, "error", err)
			continue
		}
		live := err == nil && j.State == store.JobQueued && !j.RemovedFromQueue() &&
			j.InstallationID != "" && !IsDemoID(j.InstallationID)
		if live && c.githubHeld(j.InstallationID, now) {
			continue
		}
		c.startChecksMu.Lock()
		delete(c.startChecks, p.id)
		if live {
			if c.startChecked == nil {
				c.startChecked = map[string]time.Time{}
			}
			c.startChecked[p.id] = now
		}
		c.startChecksMu.Unlock()
		if live {
			out = append(out, j)
		}
	}
	return out
}

// jobRecoveryLoop is the single owner of checking known unfinished jobs against
// GitHub, and it runs whether or not the fallback poller is on.
//
// That check is not about discovering queued work; it is the only thing that
// notices a completion whose webhook never arrived, and an in-progress job
// nothing ever completes counts against its repository's scale-up limit for
// ever and keeps its runner's host from being cleaned up. Nothing else may
// call it on a timer: a second caller doubles every GitHub call and races on
// the rotation offset. A fleet that has lost its lease is left alone for the
// same reason the poller leaves it, since it cannot act on what it would learn.
func (c *Controller) jobRecoveryLoop(ctx context.Context) {
	interval := c.pollInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if c.mayAct() {
			c.reconcileKnownJobs(ctx, c.Now())
			c.recoveries.Add(1)
		}
		if !c.awaitRecoveryTick(ctx, ticker, &interval) {
			return
		}
	}
}

// awaitRecoveryTick blocks until the next sweep is due and reports false when
// the context ended first.
//
// A settings change wakes it too, since this loop spends the most installation
// quota of any and an operator who raises github.poll_interval to relieve a
// rate limit needs it retuned now, not at the next restart. Any setting wakes
// it, so it retunes and keeps waiting rather than sweeping: a change to
// something unrelated must not cost ten GitHub calls.
func (c *Controller) awaitRecoveryTick(ctx context.Context, ticker *time.Ticker, interval *time.Duration) bool {
	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			return true
		case <-c.recoverySettingsChanged:
			if d := c.pollInterval(); d != *interval {
				*interval = d
				ticker.Reset(d)
			}
		}
	}
}
