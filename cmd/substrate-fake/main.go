// Command substrate-fake runs a fake Agent Substrate cluster: ateapi, the
// Kubernetes objects Substrate clients read, and Prometheus, on loopback.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	fake "github.com/alee792/substrate-fake"
)

func main() {
	scenario := flag.String("scenario", "demo", "demo (412 actors with problems) or basic (one empty pool)")
	names := flag.String("metric-names", "dotted", "dotted (Prometheus 3 UTF-8) or underscore")
	dir := flag.String("dir", "", "directory for the kubeconfig (default: a temporary one)")
	token := flag.String("token", "", "kubeconfig bearer token, which ateapi takes as ate-client (default: a signed ate-client token)")
	flag.Parse()

	opts := fake.Options{Dir: *dir, Token: *token}
	switch *scenario {
	case "demo":
		opts.Scenario = fake.Demo
	case "basic":
		opts.Scenario = fake.Basic
	default:
		fmt.Fprintf(os.Stderr, "unknown scenario %q\n", *scenario)
		os.Exit(2)
	}
	switch *names {
	case "dotted":
	case "underscore":
		opts.Names = fake.UnderscoreNames
	default:
		fmt.Fprintf(os.Stderr, "unknown metric names %q\n", *names)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	c, err := fake.Start(ctx, opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer c.Close()
	fmt.Printf(`substrate-fake is running the %s scenario.

  export KUBECONFIG=%s
  ateapi:      %s  (TLS for %s; the kubeconfig's token is ate-client, an owner)
  prometheus:  %s

  substrate-console serve --kubeconfig %s --endpoint %s --prometheus-url %s
`, *scenario, c.Kubeconfig, c.APIEndpoint, fake.APIServerName, c.PrometheusURL, c.Kubeconfig, c.APIEndpoint, c.PrometheusURL)
	if *scenario == "demo" {
		// Tokens for the demo's other people, valid for a day.
		dir := filepath.Dir(c.Kubeconfig)
		fmt.Println("\n  Tokens for the demo's other people:")
		for _, p := range []struct{ file, principal, what string }{
			{"oncall.token", fake.DemoOnCall, "global viewer"},
			{"triage-dev.token", fake.DemoTriageDev, "editor of triage only"},
		} {
			path := filepath.Join(dir, p.file)
			tok := c.World.IssueToken(p.principal, fake.TokenOptions{TTL: 24 * time.Hour})
			if err := os.WriteFile(path, []byte(tok+"\n"), 0o600); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			fmt.Printf("  %-22s %s  (%s)\n", p.what+":", path, p.principal)
		}
	}
	fmt.Println("\nCtrl-C stops it.")
	<-ctx.Done()
}
