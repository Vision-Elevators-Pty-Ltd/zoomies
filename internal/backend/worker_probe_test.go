package backend

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

func TestWorkerAbsenceRequiresARecognisedBoundedProcessTable(t *testing.T) {
	for _, tc := range []struct {
		name      string
		body      string
		status    int
		present   bool
		wantError bool
	}{
		{"worker present", `{"Titles":["PID","COMMAND"],"Processes":[["1","Runner.Listener"],["2","Runner.Worker"]]}`, 200, true, false},
		{"worker absent", `{"Titles":["PID","COMMAND"],"Processes":[["1","Runner.Listener"],["2","bash"]]}`, 200, false, false},
		{"probe refused", `{"message":"sensitive daemon detail"}`, 500, false, true},
		{"unsupported", `{"message":"not implemented"}`, 404, false, true},
		{"malformed", `{`, 200, false, true},
		{"wrong columns", `{"Titles":["COMMAND"],"Processes":[["Runner.Listener"]]}`, 200, false, true},
		{"arguments instead of names", `{"Titles":["PID","COMMAND"],"Processes":[["1","/runner/Runner.Worker --secret hidden"]]}`, 200, false, true},
		{"empty table", `{"Titles":["PID","COMMAND"],"Processes":[]}`, 200, false, true},
		{"incomplete row", `{"Titles":["PID","COMMAND"],"Processes":[["1","Runner.Listener"],[]]}`, 200, false, true},
		{"invalid PID", `{"Titles":["PID","COMMAND"],"Processes":[["unknown","Runner.Listener"]]}`, 200, false, true},
		{"oversized", strings.Repeat(" ", (1<<20)+1), 200, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeEngine(t, map[string]http.HandlerFunc{
				"GET " + v + "/containers/c1/top": func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Query().Get("ps_args") != "-eo pid,comm" {
						t.Error("probe requested arguments instead of process names")
					}
					w.WriteHeader(tc.status)
					_, _ = w.Write([]byte(tc.body))
				},
			})
			b := dockerBackendFor(t, f, DockerOptions{})
			present, err := b.RunnerWorkerPresent(context.Background(), "c1")
			if present != tc.present || (err != nil) != tc.wantError {
				t.Fatalf("probe = %v, %v; want present %v, error %v", present, err, tc.present, tc.wantError)
			}
			if err != nil && (strings.Contains(err.Error(), "sensitive") || strings.Contains(err.Error(), "hidden")) {
				t.Fatal("probe error disclosed daemon output")
			}
		})
	}
}
