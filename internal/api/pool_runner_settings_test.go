package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/eyupio/zoomies/internal/controller"
	"github.com/eyupio/zoomies/internal/store"
)

// A pool may override the fleet's runner timings, and the three states an
// operator can express all survive the round trip.
//
// The states are the point. Absent leaves what the pool had, so a PATCH that
// changes a priority does not clear an override nobody mentioned; null hands
// the setting back to the fleet; and a duration sets it. A shape that could
// not tell the first two apart would make "stop overriding this" impossible to
// say without also re-sending every other field.
func TestAPoolsRunnerSettingsSurviveTheRoundTrip(t *testing.T) {
	h := newHarness(t)
	inst := h.installation()
	u, _ := h.user("operator", store.RoleOperator)
	cookie := h.session(u)

	body := poolBody(inst.ID)
	body["runner_settings"] = map[string]any{
		"provision_timeout": "45m",
		"drain_timeout":     "2m",
	}
	res := h.do(request{method: http.MethodPost, path: "/api/v1/pools", cookie: cookie, body: body})
	res.mustStatus(t, http.StatusCreated, "create")
	var created controller.PoolView
	res.into(t, &created)

	if created.RunnerSettings.ProvisionTimeout == nil ||
		created.RunnerSettings.ProvisionTimeout.Duration() != 45*time.Minute {
		t.Fatalf("provision timeout = %v, want 45m", created.RunnerSettings.ProvisionTimeout)
	}
	if created.RunnerSettings.DrainTimeout == nil ||
		created.RunnerSettings.DrainTimeout.Duration() != 2*time.Minute {
		t.Fatalf("drain timeout = %v, want 2m", created.RunnerSettings.DrainTimeout)
	}
	if created.RunnerSettings.MaxRunnerLifetime != nil {
		t.Errorf("a setting nobody mentioned was given a value: %v", created.RunnerSettings.MaxRunnerLifetime)
	}

	// An edit that says nothing about the overrides leaves them alone.
	res = h.do(request{method: http.MethodPatch, path: "/api/v1/pools/" + created.ID,
		cookie: cookie, body: map[string]any{"priority": 3}})
	res.mustStatus(t, http.StatusOK, "patch priority")
	var untouched controller.PoolView
	res.into(t, &untouched)
	if untouched.RunnerSettings.ProvisionTimeout == nil ||
		untouched.RunnerSettings.ProvisionTimeout.Duration() != 45*time.Minute {
		t.Fatalf("an unrelated edit cleared an override: %+v", untouched.RunnerSettings)
	}

	// An explicit null hands that one setting back to the fleet and leaves the
	// other where it was.
	res = h.do(request{method: http.MethodPatch, path: "/api/v1/pools/" + created.ID, cookie: cookie,
		body: map[string]any{"runner_settings": map[string]any{"provision_timeout": nil}}})
	res.mustStatus(t, http.StatusOK, "patch null")
	var cleared controller.PoolView
	res.into(t, &cleared)
	if cleared.RunnerSettings.ProvisionTimeout != nil {
		t.Errorf("an explicit null left the override in place: %v", cleared.RunnerSettings.ProvisionTimeout)
	}
	if cleared.RunnerSettings.DrainTimeout == nil ||
		cleared.RunnerSettings.DrainTimeout.Duration() != 2*time.Minute {
		t.Errorf("clearing one override cleared another: %+v", cleared.RunnerSettings)
	}
}

// Zero is a real answer, not an absence: a pool on a slow air-gapped registry
// means "never fail a runner of mine for taking too long to register".
func TestAnOverrideOfZeroIsKept(t *testing.T) {
	h := newHarness(t)
	inst := h.installation()
	u, _ := h.user("operator", store.RoleOperator)
	cookie := h.session(u)

	body := poolBody(inst.ID)
	body["runner_settings"] = map[string]any{"provision_timeout": "0s"}
	res := h.do(request{method: http.MethodPost, path: "/api/v1/pools", cookie: cookie, body: body})
	res.mustStatus(t, http.StatusCreated, "create")
	var created controller.PoolView
	res.into(t, &created)
	if created.RunnerSettings.ProvisionTimeout == nil {
		t.Fatal("an override of zero was read as nothing having been said")
	}
	if created.RunnerSettings.ProvisionTimeout.Duration() != 0 {
		t.Errorf("provision timeout = %v, want 0", created.RunnerSettings.ProvisionTimeout)
	}
}

// A duration the parser cannot read is a field error naming the field that
// carried it, so the form can put the message under the input rather than
// showing a 400 about JSON.
func TestAnUnreadableOverrideNamesItsField(t *testing.T) {
	h := newHarness(t)
	inst := h.installation()
	u, _ := h.user("operator", store.RoleOperator)
	cookie := h.session(u)

	for _, tc := range []struct {
		name  string
		value any
		field string
	}{
		{"not a duration", "5 munutes", "runner_settings.provision_timeout"},
		{"negative", "-1m", "runner_settings.provision_timeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := poolBody(inst.ID)
			body["runner_settings"] = map[string]any{"provision_timeout": tc.value}
			res := h.do(request{method: http.MethodPost, path: "/api/v1/pools", cookie: cookie, body: body})
			res.mustStatus(t, http.StatusUnprocessableEntity, "create")
			var env errorEnvelope
			res.into(t, &env)
			named := false
			for _, fe := range env.Errors {
				if fe.Field == tc.field {
					named = true
				}
			}
			if !named {
				t.Errorf("no error on %s: %s", tc.field, res.body)
			}
		})
	}
}

// shm_size_mb is a number of MB with the same three states as the timings:
// absent keeps it, null hands it back to runners.shm_size_mb, and 0 is a real
// answer, Docker's own 64 MB. A negative size is refused.
func TestAPoolsShmSizeSurvivesTheRoundTrip(t *testing.T) {
	h := newHarness(t)
	inst := h.installation()
	u, _ := h.user("operator", store.RoleOperator)
	cookie := h.session(u)

	body := poolBody(inst.ID)
	body["runner_settings"] = map[string]any{"shm_size_mb": 4096}
	res := h.do(request{method: http.MethodPost, path: "/api/v1/pools", cookie: cookie, body: body})
	res.mustStatus(t, http.StatusCreated, "create")
	var created controller.PoolView
	res.into(t, &created)
	if v := created.RunnerSettings.ShmSizeMB; v == nil || *v != 4096 {
		t.Fatalf("shm_size_mb = %v, want 4096", v)
	}

	res = h.do(request{method: http.MethodPatch, path: "/api/v1/pools/" + created.ID,
		cookie: cookie, body: map[string]any{"runner_settings": map[string]any{"shm_size_mb": 0}}})
	res.mustStatus(t, http.StatusOK, "patch zero")
	var zero controller.PoolView
	res.into(t, &zero)
	if v := zero.RunnerSettings.ShmSizeMB; v == nil || *v != 0 {
		t.Fatalf("an explicit 0 became %v; it is Docker's own 64 MB, not the fleet's", v)
	}

	res = h.do(request{method: http.MethodPatch, path: "/api/v1/pools/" + created.ID,
		cookie: cookie, body: map[string]any{"runner_settings": map[string]any{"shm_size_mb": nil}}})
	res.mustStatus(t, http.StatusOK, "patch null")
	var cleared controller.PoolView
	res.into(t, &cleared)
	if cleared.RunnerSettings.ShmSizeMB != nil {
		t.Fatalf("null left shm_size_mb at %d", *cleared.RunnerSettings.ShmSizeMB)
	}

	res = h.do(request{method: http.MethodPatch, path: "/api/v1/pools/" + created.ID,
		cookie: cookie, body: map[string]any{"runner_settings": map[string]any{"shm_size_mb": -1}}})
	res.mustNotSucceed(t, "a negative shm_size_mb")
}

// runner_settings.devices: /dev/kvm only, and only on a pool that selects
// kvm=ok hosts; absent keeps it, null clears it, and the export carries it.
func TestAPoolMapsKVMOnlyOnKVMHosts(t *testing.T) {
	h := newHarness(t)
	inst := h.installation()
	u, _ := h.user("operator", store.RoleOperator)
	cookie := h.session(u)

	body := poolBody(inst.ID)
	body["runner_settings"] = map[string]any{"devices": []string{"/dev/kvm"}}
	h.do(request{method: http.MethodPost, path: "/api/v1/pools", cookie: cookie, body: body}).mustNotSucceed(t, "kvm without the kvm=ok selector")

	body["runner_settings"] = map[string]any{"devices": []string{"/dev/sda"}}
	body["host_selector"] = map[string]string{"kvm": "ok"}
	h.do(request{method: http.MethodPost, path: "/api/v1/pools", cookie: cookie, body: body}).mustNotSucceed(t, "a device that is not /dev/kvm")

	body["runner_settings"] = map[string]any{"devices": []string{"/dev/kvm"}}
	res := h.do(request{method: http.MethodPost, path: "/api/v1/pools", cookie: cookie, body: body})
	res.mustStatus(t, http.StatusCreated, "create")
	var created controller.PoolView
	res.into(t, &created)
	if d := created.RunnerSettings.Devices; len(d) != 1 || d[0] != "/dev/kvm" {
		t.Fatalf("devices = %v", d)
	}

	res = h.do(request{method: http.MethodPatch, path: "/api/v1/pools/" + created.ID,
		cookie: cookie, body: map[string]any{"runner_settings": map[string]any{"shm_size_mb": 4096}}})
	res.mustStatus(t, http.StatusOK, "patch another setting")
	var kept controller.PoolView
	res.into(t, &kept)
	if len(kept.RunnerSettings.Devices) != 1 {
		t.Fatalf("an absent devices field dropped them: %v", kept.RunnerSettings.Devices)
	}

	h.do(request{method: http.MethodPatch, path: "/api/v1/pools/" + created.ID,
		cookie: cookie, body: map[string]any{"host_selector": map[string]string{}}}).mustNotSucceed(t, "dropping kvm=ok while mapping kvm")

	doc := documentPool(&store.Pool{Name: "emulator", RunnerSettings: store.RunnerSettings{Devices: []string{"/dev/kvm"}}}, "")
	if len(doc.RunnerSettings.Devices) != 1 {
		t.Fatalf("the export dropped devices: %+v", doc.RunnerSettings)
	}

	res = h.do(request{method: http.MethodPatch, path: "/api/v1/pools/" + created.ID,
		cookie: cookie, body: map[string]any{"runner_settings": map[string]any{"devices": nil}}})
	res.mustStatus(t, http.StatusOK, "patch null")
	var cleared controller.PoolView
	res.into(t, &cleared)
	if len(cleared.RunnerSettings.Devices) != 0 {
		t.Fatalf("null left devices %v", cleared.RunnerSettings.Devices)
	}
}

// Review of #23, finding 2, the review's probe made permanent: a pool that
// maps no devices is exported with "devices": [], so importing it over a
// same-named pool that maps /dev/kvm clears the target's devices, whichever
// selector the source has. The change is stamped, so the target's runners
// made with /dev/kvm are replaced (scheduler.staleDevices).
func TestANoDeviceExportClearsAnImportedPoolsDevices(t *testing.T) {
	for _, sel := range []store.StringMap{{"kvm": "ok"}, {}} {
		t.Run(fmt.Sprint(sel), func(t *testing.T) {
			h := newHarness(t)
			inst := h.installation()
			u, _ := h.user("operator", store.RoleOperator)
			cookie := h.session(u)
			body := poolBody(inst.ID)
			body["host_selector"] = map[string]string{"kvm": "ok"}
			body["runner_settings"] = map[string]any{"devices": []string{"/dev/kvm"}}
			res := h.do(request{method: http.MethodPost, path: "/api/v1/pools", cookie: cookie, body: body})
			res.mustStatus(t, http.StatusCreated, "create the device pool")
			var created controller.PoolView
			res.into(t, &created)
			target, err := h.st.GetPool(h.ctx, created.ID)
			if err != nil {
				t.Fatal(err)
			}
			source := *target
			source.RunnerSettings = store.RunnerSettings{}
			source.HostSelector = sel
			raw, err := json.Marshal(map[string]any{"pools": []poolDocument{documentPool(&source, inst.Target)}})
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(raw), `"devices":[]`) {
				t.Fatalf("the export of a pool with no devices does not say so: %s", raw)
			}
			h.do(request{method: http.MethodPost, path: "/api/v1/pools/import", cookie: cookie,
				body: map[string]any{"document": string(raw)}}).mustStatus(t, http.StatusOK, "import")
			after, err := h.st.GetPool(h.ctx, created.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(after.RunnerSettings.Devices) != 0 {
				t.Fatalf("the imported pool kept devices %v", after.RunnerSettings.Devices)
			}
			if at := after.RunnerSettings.DevicesChangedAt; at == nil || at.IsZero() {
				t.Fatal("clearing the devices by import was not stamped")
			}
		})
	}
}

// Review of #23, finding 1: a PATCH that changes the devices stamps the
// change; one that leaves them alone does not move the stamp.
func TestChangingAPoolsDevicesIsStamped(t *testing.T) {
	h := newHarness(t)
	inst := h.installation()
	u, _ := h.user("operator", store.RoleOperator)
	cookie := h.session(u)
	body := poolBody(inst.ID)
	body["host_selector"] = map[string]string{"kvm": "ok"}
	res := h.do(request{method: http.MethodPost, path: "/api/v1/pools", cookie: cookie, body: body})
	res.mustStatus(t, http.StatusCreated, "create")
	var created controller.PoolView
	res.into(t, &created)
	stamp := func() *time.Time {
		p, err := h.st.GetPool(h.ctx, created.ID)
		if err != nil {
			t.Fatal(err)
		}
		return p.RunnerSettings.DevicesChangedAt
	}
	if stamp() != nil {
		t.Fatal("a pool that never mapped devices is stamped")
	}
	patch := func(rs map[string]any) {
		h.do(request{method: http.MethodPatch, path: "/api/v1/pools/" + created.ID, cookie: cookie,
			body: map[string]any{"runner_settings": rs}}).mustStatus(t, http.StatusOK, "patch")
	}
	patch(map[string]any{"devices": []string{"/dev/kvm"}})
	added := stamp()
	if added == nil || added.IsZero() {
		t.Fatal("adding /dev/kvm was not stamped")
	}
	patch(map[string]any{"shm_size_mb": 2048})
	patch(map[string]any{"devices": []string{"/dev/kvm"}})
	if got := stamp(); got == nil || !got.Equal(*added) {
		t.Fatalf("an unchanged device list moved the stamp from %v to %v", added, got)
	}
	time.Sleep(2 * time.Millisecond)
	patch(map[string]any{"devices": nil})
	if got := stamp(); got == nil || !got.After(*added) {
		t.Fatalf("clearing the devices did not move the stamp: %v", got)
	}
}

// Review of #23, finding 3: a pool that maps devices must use docker_mode
// none, through the API as on the agent.
func TestADevicePoolMustUseDockerModeNone(t *testing.T) {
	h := newHarness(t)
	inst := h.installation()
	u, _ := h.user("operator", store.RoleOperator)
	cookie := h.session(u)
	for _, mode := range []string{"dind", "host-socket"} {
		body := poolBody(inst.ID)
		body["name"] = "emulator-" + mode
		body["docker_mode"] = mode
		body["host_selector"] = map[string]string{"kvm": "ok"}
		body["runner_settings"] = map[string]any{"devices": []string{"/dev/kvm"}}
		res := h.do(request{method: http.MethodPost, path: "/api/v1/pools", cookie: cookie, body: body})
		res.mustNotSucceed(t, "devices with docker_mode "+mode)
		if !strings.Contains(string(res.body), "docker_mode none") {
			t.Fatalf("%s: the refusal does not say why: %s", mode, res.body)
		}
	}
}
