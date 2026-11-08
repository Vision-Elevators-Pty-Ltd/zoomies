package config

import (
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/eyupio/zoomies/internal/scheduler"
	"github.com/eyupio/zoomies/internal/store"
)

// The enum settings Validate refuses a wrong value of, each with how to put a
// value straight into the field. They go straight in, not through SetValueString,
// because that normalises a value before it is stored and a file does not.
var validatedEnums = map[string]func(c *Config, v string){
	"scheduler.history_sizing":         func(c *Config, v string) { c.Scheduler.HistorySizing = v },
	"scheduler.history_raise":          func(c *Config, v string) { c.Scheduler.HistoryRaise = v },
	"scheduler.host_order":             func(c *Config, v string) { c.Scheduler.HostOrder = v },
	"scheduler.memory_booking":         func(c *Config, v string) { c.Scheduler.MemoryBooking = v },
	"scheduler.memory_admission":       func(c *Config, v string) { c.Scheduler.MemoryAdmission = v },
	"scheduler.working_set_history":    func(c *Config, v string) { c.Scheduler.WorkingSetHistory = v },
	"scheduler.working_set_sampling":   func(c *Config, v string) { c.Scheduler.WorkingSetSampling = v },
	"scheduler.placement_mode":         func(c *Config, v string) { c.Scheduler.PlacementMode = v },
	"scheduler.size_routing":           func(c *Config, v string) { c.Scheduler.SizeRouting = v },
	"scheduler.auto_pools":             func(c *Config, v string) { c.Scheduler.AutoPools = v },
	"scheduler.auto_pools_docker_mode": func(c *Config, v string) { c.Scheduler.AutoPoolsDockerMode = v },
	"scheduler.size_default_class":     func(c *Config, v string) { c.Scheduler.SizeDefaultClass = v },
	"security.auto_apply_remedies":     func(c *Config, v string) { c.Security.AutoApplyRemedies = v },
}

func findingsFor(c *Config, code string) Findings {
	var out Findings
	for _, f := range c.Validate() {
		if f.Code == code {
			out = append(out, f)
		}
	}
	return out
}

// Validate used to restate each of these lists by hand beside the settings
// registry, and the two had to be kept equal by whoever remembered. The
// validator now asks the registry, and this holds that: every choice, and every
// older spelling, is accepted; anything else is refused with the choices named.
func TestTheValidatorAcceptsExactlyWhatTheRegistryListsForEachEnum(t *testing.T) {
	for key, set := range validatedEnums {
		s, ok := LookupSetting(key)
		if !ok || s.Kind != KindEnum || len(s.Choices) == 0 {
			t.Errorf("%s is not an enum setting in the registry, so Validate cannot be asking it", key)
			continue
		}
		accepted := append([]string{""}, s.Choices...)
		for alias := range s.Aliases {
			accepted = append(accepted, alias)
		}
		for _, value := range accepted {
			c := Default()
			set(c, value)
			if got := findingsFor(c, key); len(got) != 0 {
				t.Errorf("%s=%q is a choice or alias the registry lists, and Validate refused it: %s", key, value, got[0].Title)
			}
		}
		c := Default()
		set(c, "bogus")
		got := findingsFor(c, key)
		if len(got) != 1 || got[0].Severity != SeverityError {
			t.Errorf("%s=bogus should be one error finding, got %+v", key, got)
			continue
		}
		if !strings.HasPrefix(got[0].Fix, "choose ") {
			t.Errorf("%s: the fix %q does not start with \"choose \"", key, got[0].Fix)
		}
		for _, choice := range s.Choices {
			if !strings.Contains(got[0].Fix, choice) {
				t.Errorf("%s: the fix %q does not name the choice %q", key, got[0].Fix, choice)
			}
		}
	}
}

// security.auto_apply_remedies accepted yes and no from the settings page (the
// registry lists them) and from the runtime (AutoApplyMode reads them), and
// Validate refused both as an error, which stops startup: a zoomies.yaml that
// said `auto_apply_remedies: yes` could not start the controller. The three
// places have to agree about every spelling.
func TestEverySpellingOfTheAutomaticApplyModeIsReadTheSameInAllThreePlaces(t *testing.T) {
	s, _ := LookupSetting("security.auto_apply_remedies")
	if len(s.Aliases) == 0 {
		t.Fatal("security.auto_apply_remedies has no aliases any more; this test has nothing to compare")
	}
	for spelling, canonical := range s.Aliases {
		for _, written := range []string{spelling, strings.ToUpper(spelling), " " + spelling + " "} {
			c := Default()
			c.Security.AutoApplyRemedies = written
			if got := findingsFor(c, "security.auto_apply_remedies"); len(got) != 0 {
				t.Errorf("%q is an alias of %q in the registry and Validate refused it, which would stop the controller starting", written, canonical)
			}
			if got := c.Security.AutoApplyMode(); got != canonical {
				t.Errorf("%q is read as %q at runtime but the registry says it means %q", written, got, canonical)
			}
		}
	}
}

// The registry's lists are also what the scheduler's own constants say, and the
// two live in different packages. config cannot import the scheduler in its
// non-test code, which is why they were copied; a test can.
func TestTheRegistryChoicesAreTheOnesTheSchedulerAndStoreDefine(t *testing.T) {
	choices := func(key string) []string {
		s, ok := LookupSetting(key)
		if !ok {
			t.Fatalf("no setting %s", key)
		}
		return s.Choices
	}
	sameSet := func(key string, want []string) {
		t.Helper()
		got := append([]string{}, choices(key)...)
		want = append([]string{}, want...)
		sort.Strings(got)
		sort.Strings(want)
		if !slices.Equal(got, want) {
			t.Errorf("%s offers %v and the code that acts on it knows %v", key, got, want)
		}
	}
	sameSet("scheduler.host_order", scheduler.HostOrders)
	sameSet("scheduler.memory_booking", scheduler.BookingModes)
	sameSet("scheduler.memory_admission", scheduler.AdmissionModes)
	sameSet("scheduler.history_raise", scheduler.HistoryRaiseModes)
	sameSet("scheduler.size_routing", scheduler.SizeModes)
	sameSet("scheduler.auto_pools", scheduler.SizeModes)
	sameSet("scheduler.size_default_class", []string{string(store.SizeSmall), string(store.SizeMedium), string(store.SizeLarge)})
}
