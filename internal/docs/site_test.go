package docs

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The partial's own comment explains the attribute it leaves out, so the check
// has to read the markup and not the explanation.
var jinjaComment = regexp.MustCompile(`(?s)\{#.*?#\}`)

// The site's header names the latest release. Material's script used to fill
// it in from the GitHub API and keep the answer in the tab's sessionStorage,
// which a browser restores with the tab: a tab first opened when V1.1.0 was
// the newest release said V1.1.0 two releases later. The facts are resolved
// when the site is built now, and these are the pieces that keep that true.

func TestTheHeaderFactsAreWrittenAtBuildTimeNotFetchedByTheTheme(t *testing.T) {
	partial, err := os.ReadFile("../../overrides/partials/source.html")
	if err != nil {
		t.Fatalf("the source partial override is missing: %v", err)
	}
	markup := jinjaComment.ReplaceAllString(string(partial), "")
	if strings.Contains(markup, "data-md-component") {
		t.Error("overrides/partials/source.html carries data-md-component, so Material's script will mount on it and replace the built facts with whatever the tab cached")
	}
	if !strings.Contains(markup, "md-source__fact--") {
		t.Error("overrides/partials/source.html renders no facts, so the header names no release at all")
	}
	config, err := os.ReadFile("../../mkdocs.yml")
	if err != nil {
		t.Fatalf("reading mkdocs.yml: %v", err)
	}
	if !strings.Contains(string(config), "- hooks/source.py") {
		t.Error("mkdocs.yml does not register hooks/source.py, so nothing resolves the facts the partial renders")
	}
}

func TestTheSiteIsRebuiltAndPublishedWhenAReleaseIsPublished(t *testing.T) {
	body := workflowFiles(t)["docs.yml"]
	if body == "" {
		t.Fatal("docs.yml is missing")
	}
	for _, want := range []struct {
		needle, why string
	}{
		{"release:\n    types: [published]", "a release that does not rebuild the site leaves the header naming the one before"},
		{`gh workflow run docs.yml --repo "$REPO" --ref main`, "a release must request a run whose ref is main, not just check out main in a tag run"},
		{"GITHUB_TOKEN: ${{ github.token }}", "anonymous API requests share a rate limit a shared runner's address can already have spent"},
		{`grep -q "md-source__fact--version" site/index.html`, "an offline build is allowed to succeed without the facts; the published site is not"},
	} {
		if !strings.Contains(body, want.needle) {
			t.Errorf("docs.yml has no %q: %s", want.needle, want.why)
		}
	}
}

// The site is built from more than docs/. The hook publishes the skills under
// /.well-known/agent-skills and digests them, the worker's tests run in the
// build, and the installers are copied to the site root. A change to one of
// them that does not start the build is not published until the daily rebuild,
// and a pull request that breaks one is not checked at all.
func TestTheSiteWorkflowStartsForEveryInputToTheBuild(t *testing.T) {
	var workflow struct {
		On struct {
			Push struct {
				Paths []string `yaml:"paths"`
			} `yaml:"push"`
			PullRequest struct {
				Paths []string `yaml:"paths"`
			} `yaml:"pull_request"`
		} `yaml:"on"`
	}
	if err := yaml.Unmarshal([]byte(workflowFiles(t)["docs.yml"]), &workflow); err != nil {
		t.Fatalf("parsing docs.yml: %v", err)
	}
	events := map[string][]string{
		"push":         workflow.On.Push.Paths,
		"pull_request": workflow.On.PullRequest.Paths,
	}
	for _, want := range []struct{ path, why string }{
		{"docs/**", "the pages"},
		{"overrides/**", "the theme's templates"},
		{"hooks/**", "the hooks that write llms.txt, the discovery documents and auth.md"},
		{"site-worker/**", "the edge worker, whose tests run in this build"},
		{"skills/**", "the skills the hook publishes under /.well-known/agent-skills and pins by digest"},
		{"mkdocs.yml", "the site's configuration"},
		{"install.sh", "copied to the site root"},
		{"install.ps1", "copied to the site root"},
		{"connect-proxmox.sh", "copied to the site root"},
		{".github/workflows/docs.yml", "the workflow itself"},
	} {
		for event, paths := range events {
			if !slices.Contains(paths, want.path) {
				t.Errorf("docs.yml does not list %q under %s, so a change to it does not start the build: %s", want.path, event, want.why)
			}
		}
	}
}

// v1.3.1 built successfully but Pages rejected its tag before Deploy could
// start. Checking out main cannot change the run's deployment identity.
func TestReleaseRefreshKeepsTagsOutOfThePagesEnvironment(t *testing.T) {
	body := workflowFiles(t)["docs.yml"]
	_, jobs, _ := strings.Cut(body, "\njobs:\n")
	refresh, rest, ok := strings.Cut(jobs, "\n  build:\n")
	if !ok {
		t.Fatal("the website workflow has no build job")
	}
	// The build job deploys the site to the edge itself, so the step that
	// publishes is in it and not in a job of its own.
	build, deploy := rest, rest
	for _, want := range []string{
		"if: github.event.action != 'closed' && github.repository == 'eyupio/zoomies' && github.event_name == 'release'",
		"actions: write",
		"GH_TOKEN: ${{ github.token }}",
	} {
		if !strings.Contains(refresh, want) {
			t.Errorf("release refresh is missing %q", want)
		}
	}
	if strings.Contains(refresh, "environment:") || strings.Contains(refresh, "actions/checkout@") {
		t.Error("release refresh must dispatch main without deploying or executing tag contents")
	}
	if !strings.Contains(build, "if: github.event.action != 'closed' && github.event_name != 'release'") {
		t.Error("release tags still build a Pages artefact instead of leaving it to the main-branch run")
	}
	if !strings.Contains(deploy, "if: github.repository == 'eyupio/zoomies' && github.event_name != 'pull_request' && github.ref == 'refs/heads/main'") {
		t.Error("Pages must only deploy from upstream main, including when a release requested the run")
	}
	if strings.Contains(build, "actions: write") || strings.Contains(deploy, "actions: write") {
		t.Error("only the release refresh job needs permission to dispatch a workflow")
	}
}
