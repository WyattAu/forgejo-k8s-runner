package main

import (
	"context"
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
	cfgPath := "config.yaml"
	if len(os.Args) > 1 { cfgPath = os.Args[1] }

	cfg, err := config.LoadDefault(cfgPath)
	if err != nil { log.Fatalf("config: %v", err) }
	if cfg.Runner.FetchInterval == 0 { cfg.Runner.FetchInterval = 2 * time.Second }
	if cfg.Runner.FetchTimeout == 0 { cfg.Runner.FetchTimeout = 5 * time.Second }

	reg, err := config.LoadRegistration(cfg.Runner.File)
	if err != nil { log.Fatalf("reg: %v (run act_runner register first)", err) }

	log.Printf("Starting K8s runner %q (cap=%d)", reg.Name, cfg.Runner.Capacity)

	if err := initK8s(cfg.K8s.Kubeconfig); err != nil { log.Fatalf("k8s: %v", err) }

	cli := client.New(reg.Address, cfg.Runner.Insecure, reg.UUID, reg.Token, ver.Version())
	setK8sContext(cli, cfg.K8s.Namespace)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	poller := poll.New(cfg, cli, cfg.Runner.Capacity)
	poller.SetTaskHandler(k8sTaskHandler)
	log.Println("Polling for tasks...")
	poller.Poll()
	<-ctx.Done()
	poller.Shutdown(context.Background())
}
