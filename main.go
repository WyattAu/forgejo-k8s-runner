// forgejo-k8s-runner — K8s-native Forgejo Actions runner.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/WyattAu/forgejo-k8s-runner/app/poll"
	"github.com/WyattAu/forgejo-k8s-runner/pkg/client"
	"github.com/WyattAu/forgejo-k8s-runner/pkg/config"
	"github.com/WyattAu/forgejo-k8s-runner/pkg/ver"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: forgejo-k8s-runner <daemon|register> [config.yaml]")
		os.Exit(1)
	}

	cfgPath := "config.yaml"
	if len(os.Args) > 2 {
		cfgPath = os.Args[2]
	}

	cfg, err := config.LoadConfig(cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	if cfg.Runner.FetchInterval == 0 {
		cfg.Runner.FetchInterval = 2 * time.Second
	}
	if cfg.Runner.FetchTimeout == 0 {
		cfg.Runner.FetchTimeout = 5 * time.Second
	}

	reg, err := config.LoadRegistration(cfg.Runner.File)
	if err != nil {
		log.Fatalf("load registration: %v (run 'register' first)", err)
	}

	switch os.Args[1] {
	case "register":
		if err := config.DoRegistration(cfg, reg); err != nil {
			log.Fatalf("registration failed: %v", err)
		}
		log.Println("Runner registered successfully")

	case "daemon":
		log.Printf("Starting K8s runner %q (capacity=%d, version=%s)",
			reg.Name, cfg.Runner.Capacity, ver.Version())

		if err := initK8s(cfg.K8s.Kubeconfig); err != nil {
			log.Fatalf("k8s client: %v", err)
		}
		setK8sContext(cli, cfg.K8s.Namespace)

		cli, err := client.New(reg.Address, cfg.Runner.Insecure, reg.UUID, reg.Token, ver.Version())
		if err != nil {
			log.Fatalf("connect to Forgejo: %v", err)
		}
		defer cli.Close()

		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer cancel()

		poller := poll.New(cfg, cli, cfg.Runner.Capacity, k8sTaskHandler)
		log.Println("Runner daemon started, polling for tasks...")
		poller.Poll()

		<-ctx.Done()
		log.Println("Shutting down...")
		poller.Shutdown(context.Background())

	default:
		log.Fatalf("unknown command: %s", os.Args[1])
	}
}
