/*
Copyright 2026 pgcopydb-operator contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/test/dashboards"
)

// metricsMigration is the follow Migration the metrics specs drive.
const metricsMigration = "e2e-metrics"

// promLocalPort is the local end of the suite-spawned port-forward; high and
// odd so it stays clear of a developer's own forwards.
const promLocalPort = "19090"

var (
	// promBase is the Prometheus base URL every query goes to.
	promBase string
	// promPF is non-nil when the suite owns a kubectl port-forward.
	promPF *promForward
	// promHTTP bounds every call so a wedged endpoint fails a spec instead of
	// hanging it past every Eventually.
	promHTTP = &http.Client{Timeout: 30 * time.Second}
)

// promForward is the kubectl port-forward E2E_PROMETHEUS_PORT_FORWARD spawns.
type promForward struct {
	namespace, service, port string
	cmd                      *exec.Cmd
}

// parsePromForward splits "namespace/service:port".
func parsePromForward(spec string) (*promForward, error) {
	ns, rest, slash := strings.Cut(spec, "/")
	svc, port, colon := strings.Cut(rest, ":")
	if _, err := strconv.Atoi(port); !slash || !colon || ns == "" || svc == "" || err != nil {
		return nil, fmt.Errorf("E2E_PROMETHEUS_PORT_FORWARD must be namespace/service:port, got %q", spec)
	}
	return &promForward{namespace: ns, service: svc, port: port}, nil
}

// start spawns the forward and polls Prometheus readiness through it, so the
// first query never races the tunnel.
func (p *promForward) start() error {
	cmd := exec.Command("kubectl", "port-forward", "-n", p.namespace,
		"svc/"+p.service, promLocalPort+":"+p.port)
	cmd.Stdout, cmd.Stderr = GinkgoWriter, GinkgoWriter
	if err := cmd.Start(); err != nil {
		return err
	}
	p.cmd = cmd
	for deadline := time.Now().Add(time.Minute); time.Now().Before(deadline); time.Sleep(time.Second) {
		resp, err := promHTTP.Get("http://127.0.0.1:" + promLocalPort + "/-/ready")
		if err != nil {
			continue
		}
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return nil
		}
	}
	p.stop()
	return fmt.Errorf("port-forward to %s/%s never became ready", p.namespace, p.service)
}

func (p *promForward) stop() {
	if p.cmd == nil {
		return
	}
	_ = p.cmd.Process.Kill()
	_ = p.cmd.Wait()
	p.cmd = nil
}

// promResponse is the slice of the Prometheus API envelope the specs read.
type promResponse struct {
	Status string `json:"status"`
	Error  string `json:"error"`
	Data   struct {
		Result []struct {
			Value []any `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

// promQuery runs one instant query; the caller judges HTTP code and status.
func promQuery(expr string) (int, *promResponse, error) {
	form := url.Values{"query": {expr}}
	resp, err := promHTTP.PostForm(promBase+"/api/v1/query", form)
	if errors.Is(err, syscall.ECONNREFUSED) && promPF != nil {
		// kubectl drops long-lived forwards now and then; one respawn covers
		// a mid-suite death without hiding a Prometheus that is really gone.
		promPF.stop()
		if err := promPF.start(); err != nil {
			return 0, nil, err
		}
		resp, err = promHTTP.PostForm(promBase+"/api/v1/query", form)
	}
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	pr := &promResponse{}
	if err := json.NewDecoder(resp.Body).Decode(pr); err != nil {
		return resp.StatusCode, nil, fmt.Errorf("decode response for %s: %w", expr, err)
	}
	return resp.StatusCode, pr, nil
}

// promVector asserts the query succeeds and returns its sample values.
func promVector(g Gomega, expr string) []float64 {
	code, pr, err := promQuery(expr)
	g.Expect(err).NotTo(HaveOccurred(), "query %s", expr)
	g.Expect(code).To(Equal(http.StatusOK), "query %s: %s", expr, promErr(pr))
	g.Expect(pr.Status).To(Equal("success"), "query %s: %s", expr, promErr(pr))
	out := make([]float64, 0, len(pr.Data.Result))
	for _, r := range pr.Data.Result {
		g.Expect(r.Value).To(HaveLen(2), "malformed sample for %s", expr)
		v, perr := strconv.ParseFloat(fmt.Sprint(r.Value[1]), 64)
		g.Expect(perr).NotTo(HaveOccurred(), "unparseable sample value for %s", expr)
		out = append(out, v)
	}
	return out
}

func promErr(pr *promResponse) string {
	if pr == nil {
		return "(no body)"
	}
	return pr.Error
}

// promValue returns the value of the exactly-one series the query must match.
func promValue(g Gomega, expr string) float64 {
	s := promVector(g, expr)
	g.Expect(s).To(HaveLen(1), "want exactly one series for %s", expr)
	return s[0]
}

// metricsJob derives the e2e install's Prometheus job label: a ServiceMonitor
// target's job defaults to its Service name, and the chart names the metrics
// Service <fullname>-metrics with fullname = release[-chartname].
func metricsJob() string {
	full := helmRelease
	if chart := filepath.Base(chartPath); !strings.Contains(full, chart) {
		full += "-" + chart
	}
	return full + "-metrics"
}

// e2eSeries pins a metric to the e2e install's job label and the metrics
// Migration. The production operator on the shared cluster co-reconciles the
// same Migration and double-exports its series; only the job tells them apart.
func e2eSeries(metric string) string {
	return fmt.Sprintf("%s{job=%q, namespace=%q, name=%q}", metric, metricsJob(), nsE2E, metricsMigration)
}

// panelKey identifies a dashboard panel for the emptyOK allowlist.
type panelKey struct{ uid, title string }

// The chart's dashboard uids, as the JSON files declare them.
const (
	uidFleet    = "pgcopydb-fleet"
	uidDetail   = "pgcopydb-migration"
	uidOperator = "pgcopydb-operator"
)

// emptyOK lists the panels that are legitimately empty for a healthy,
// completed migration; every other panel must return data.
var emptyOK = map[panelKey]bool{
	// The sweep runs once every spec migration is Completed, so the sum over
	// the in-flight phases has no series left to add up.
	{uid: uidFleet, title: "Active"}: true,
	// A Failed series would have failed its own spec first; none is the point.
	{uid: uidFleet, title: "Failed"}: true,
	// The suite never leaves a migration suspended.
	{uid: uidFleet, title: "Suspended"}: true,
	// endpos reaches status through a sentinel sample taken while the worker
	// drains; a small drain can finish before the next sample, leaving the
	// endpos gauge legitimately unset.
	{uid: uidDetail, title: "Cutover drain"}: true,
	// The same endpos gap empties that target here; the panel's other three
	// targets are asserted directly by the streaming spec.
	{uid: uidDetail, title: "LSN positions"}: true,
	// The e2e install runs with leaderElection.enabled=false, so the
	// leader-election gauge never gets a series.
	{uid: uidOperator, title: "Leader elected"}: true,
}

// panelFailure replays one panel target and describes what is wrong with the
// answer, or returns "" when the panel passes.
func panelFailure(d *dashboards.Dashboard, title, expr string) string {
	code, pr, err := promQuery(expr)
	switch {
	case err != nil:
		return fmt.Sprintf("%q: %v", title, err)
	case code != http.StatusOK || pr.Status != "success":
		return fmt.Sprintf("%q: HTTP %d, status %q: %s (%s)", title, code, pr.Status, promErr(pr), expr)
	case len(pr.Data.Result) == 0 && !emptyOK[panelKey{uid: d.UID, title: title}]:
		return fmt.Sprintf("%q: empty result (%s)", title, expr)
	}
	return ""
}

// The metrics specs prove the whole export path against a real Prometheus:
// scrape health, the live series of a streaming migration, the terminal
// series after cutover, every dashboard panel query, and series removal on
// deletion. They need a Prometheus that scrapes the suite's operator install
// (the chart's ServiceMonitor, which BeforeSuite enables).
var _ = Describe("Migration metrics", Ordered, Label("metrics"), func() {
	BeforeAll(func() {
		urlEnv := os.Getenv("E2E_PROMETHEUS_URL")
		pfEnv := os.Getenv("E2E_PROMETHEUS_PORT_FORWARD")
		switch {
		case urlEnv != "":
			promBase = strings.TrimRight(urlEnv, "/")
		case pfEnv != "":
			pf, err := parsePromForward(pfEnv)
			Expect(err).NotTo(HaveOccurred())
			Expect(pf.start()).To(Succeed())
			DeferCleanup(pf.stop)
			promPF = pf
			promBase = "http://127.0.0.1:" + promLocalPort
		default:
			Skip("no Prometheus configured: set E2E_PROMETHEUS_URL (base URL) or" +
				" E2E_PROMETHEUS_PORT_FORWARD (namespace/service:port) to run the metrics specs")
		}
		// A knob that is set but points nowhere is a misconfiguration and must
		// be red, not skipped.
		resp, err := promHTTP.Get(promBase + "/api/v1/status/buildinfo")
		Expect(err).NotTo(HaveOccurred(), "Prometheus at %s unreachable", promBase)
		defer func() { _ = resp.Body.Close() }()
		Expect(resp.StatusCode).To(Equal(http.StatusOK), "Prometheus buildinfo probe at %s", promBase)
	})

	It("has a healthy scrape of the suite's operator", func() {
		// One crisp failure here beats forty empty-series timeouts later: a
		// broken scrape (ServiceMonitor missing, scraper RBAC, network) makes
		// every query below come back empty.
		expr := fmt.Sprintf("max(up{job=%q, namespace=%q})", metricsJob(), nsOperator)
		Eventually(func(g Gomega) {
			g.Expect(promValue(g, expr)).To(Equal(1.0), "operator scrape target down")
		}, 3*time.Minute, 5*time.Second).Should(Succeed())
	})

	It("exports live series while streaming", func() {
		mig := newFollowMigration(metricsMigration, v1beta1.CutoverManual)
		mig.Spec.Verification = &v1beta1.VerificationOptions{Schema: true}
		// Same budget as the streaming specs: 0.18's catalog layer crashes
		// probabilistically at this scale; retries resume from the work dir.
		mig.Spec.BackoffLimit = 5
		create(mig)

		By("waiting for the base copy to finish and streaming to start")
		waitPhase(metricsMigration, nsE2E, migrationTimeout, v1beta1.PhaseStreaming, v1beta1.PhaseCutoverPending)

		By("asserting exactly one phase series is active")
		Eventually(func(g Gomega) {
			g.Expect(promVector(g, e2eSeries("pgcopydb_migration_phase")+" == 1")).To(HaveLen(1))
		}, 5*time.Minute, 5*time.Second).Should(Succeed())

		By("asserting plausible sampled database sizes")
		Eventually(func(g Gomega) {
			for _, m := range []string{
				"pgcopydb_migration_source_database_size_bytes",
				"pgcopydb_migration_target_database_size_bytes",
			} {
				v := promValue(g, e2eSeries(m))
				g.Expect(v).To(BeNumerically(">", 1<<20), "%s implausibly small", m)
				g.Expect(v).To(BeNumerically("<", 1<<40), "%s implausibly large", m)
			}
		}, 5*time.Minute, 5*time.Second).Should(Succeed())

		By("asserting byte progress from the gated clone poll")
		Eventually(func(g Gomega) {
			copied := promValue(g, e2eSeries("pgcopydb_migration_clone_copied_bytes"))
			planned := promValue(g, e2eSeries("pgcopydb_migration_clone_planned_bytes"))
			g.Expect(copied).To(BeNumerically(">", 0))
			g.Expect(copied).To(BeNumerically("<=", planned))
		}, 5*time.Minute, 5*time.Second).Should(Succeed())

		By("asserting table progress")
		Eventually(func(g Gomega) {
			done := promValue(g, e2eSeries("pgcopydb_migration_tables_done"))
			total := promValue(g, e2eSeries("pgcopydb_migration_tables_total"))
			g.Expect(total).To(BeNumerically(">", 0))
			g.Expect(done).To(BeNumerically("<=", total))
		}, 5*time.Minute, 5*time.Second).Should(Succeed())

		By("asserting the LSN gauges and the derived lags")
		Eventually(func(g Gomega) {
			for _, m := range []string{
				"pgcopydb_migration_source_lsn_bytes",
				"pgcopydb_migration_write_lsn_bytes",
				"pgcopydb_migration_replay_lsn_bytes",
			} {
				g.Expect(promValue(g, e2eSeries(m))).To(BeNumerically(">", 0), m)
			}
			// Differences as single queries: both operands then come from one
			// evaluation instant, so a scrape cannot land in between.
			g.Expect(promValue(g, e2eSeries("pgcopydb_migration_source_lsn_bytes")+" - "+
				e2eSeries("pgcopydb_migration_replay_lsn_bytes"))).
				To(BeNumerically(">=", 0), "source LSN behind replay LSN")
			g.Expect(promValue(g, e2eSeries("pgcopydb_migration_source_lsn_bytes")+" - "+
				e2eSeries("pgcopydb_migration_write_lsn_bytes"))).
				To(BeNumerically(">=", 0), "negative receive lag")
		}, 5*time.Minute, 5*time.Second).Should(Succeed())
	})

	It("records the terminal series after cutover", func() {
		By("approving the cutover and waiting for completion")
		waitPhase(metricsMigration, nsE2E, migrationTimeout, v1beta1.PhaseCutoverPending)
		approveCutover(metricsMigration)
		waitPhase(metricsMigration, nsE2E, migrationTimeout, v1beta1.PhaseCompleted)

		Eventually(func(g Gomega) {
			start := promValue(g, e2eSeries("pgcopydb_migration_start_time_seconds"))
			completion := promValue(g, e2eSeries("pgcopydb_migration_completion_time_seconds"))
			g.Expect(completion).To(BeNumerically(">", start), "completed before it started")
			g.Expect(completion).To(BeNumerically("~", float64(time.Now().Unix()), (30*time.Minute).Seconds()),
				"completion timestamp not from this run")
			info := fmt.Sprintf("pgcopydb_migration_info{job=%q, namespace=%q, name=%q, mode=\"follow\"}",
				metricsJob(), nsE2E, metricsMigration)
			g.Expect(promValue(g, info)).To(Equal(1.0))
			g.Expect(promValue(g, e2eSeries("pgcopydb_migration_verified"))).To(Equal(1.0))
			g.Expect(promValue(g, e2eSeries("pgcopydb_migration_attempts"))).To(BeNumerically(">=", 1))
		}, 3*time.Minute, 5*time.Second).Should(Succeed())

		By("cross-checking the progress poll landed in status, without Prometheus")
		m := &v1beta1.Migration{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nsE2E, Name: metricsMigration}, m)).To(Succeed())
		Expect(m.Status.Progress).NotTo(BeNil(), "status.progress never populated; the progress poll did not run")
		Expect(m.Status.Progress.BytesDone).NotTo(BeNil(), "status.progress.bytesDone absent")
		Expect(m.Status.Progress.BytesDone.Value()).To(BeNumerically(">", 0))
	})

	It("answers every dashboard panel query", func() {
		loaded, err := dashboards.Load(filepath.Join(chartPath, "dashboards"))
		Expect(err).NotTo(HaveOccurred())
		vars := map[string]string{"namespace": nsE2E, "name": metricsMigration, "job": metricsJob()}
		// One Eventually around the whole sweep: each pass reports every
		// failing panel, so one run shows the full damage instead of the
		// first broken panel per attempt.
		Eventually(func(g Gomega) {
			var failures []string
			for _, file := range slices.Sorted(maps.Keys(loaded)) {
				d := loaded[file]
				for _, p := range d.AllPanels() {
					for _, t := range p.Targets {
						if t.Expr == "" {
							continue
						}
						if msg := panelFailure(d, p.Title, dashboards.Substitute(t.Expr, vars)); msg != "" {
							failures = append(failures, file+" "+msg)
						}
					}
				}
			}
			g.Expect(failures).To(BeEmpty(), "failing panels:\n"+strings.Join(failures, "\n"))
		}, 3*time.Minute, 10*time.Second).Should(Succeed())
	})

	It("drops the series once the Migration is deleted", func() {
		deleteMigration(metricsMigration)
		// Through a real scrape cycle, so this catches both a missed Forget
		// and a scrape that keeps re-exporting forgotten series.
		Eventually(func(g Gomega) {
			code, pr, err := promQuery(e2eSeries("pgcopydb_migration_phase"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(code).To(Equal(http.StatusOK))
			g.Expect(pr.Status).To(Equal("success"))
			g.Expect(pr.Data.Result).To(BeEmpty(), "phase series outlived the Migration")
		}, 3*time.Minute, 5*time.Second).Should(Succeed())
	})
})
