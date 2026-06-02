package poll

import (
	"context"
	"sync"
	"sync/atomic"

	runnerv1 "code.gitea.io/actions-proto-go/runner/v1"
	"connectrpc.com/connect"
	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"

	"github.com/WyattAu/forgejo-k8s-runner/pkg/client"
	"github.com/WyattAu/forgejo-k8s-runner/pkg/config"
)

type TaskHandler func(context.Context, *runnerv1.Task)

type Poller struct {
	client     client.Client
	cfg        *config.Config
	taskHandler TaskHandler
	tasksVersion atomic.Int64
	pollingCtx context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

func New(cfg *config.Config, cli client.Client, capacity int) *Poller {
	ctx, cancel := context.WithCancel(context.Background())
	return &Poller{
		client:     cli,
		cfg:        cfg,
		pollingCtx: ctx,
		cancel:     cancel,
	}
}

func (p *Poller) SetTaskHandler(h TaskHandler) { p.taskHandler = h }

func (p *Poller) Poll() {
	limiter := rate.NewLimiter(rate.Every(p.cfg.Runner.FetchInterval), 1)
	for i := 0; i < p.cfg.Runner.Capacity; i++ {
		p.wg.Add(1)
		go p.pollLoop(limiter)
	}
	p.wg.Wait()
}

func (p *Poller) pollLoop(limiter *rate.Limiter) {
	defer p.wg.Done()
	for {
		if err := limiter.Wait(p.pollingCtx); err != nil { return }
		task, ok := p.fetchTask(p.pollingCtx)
		if !ok || task == nil { continue }
		func() {
			defer func() {
				if r := recover(); r != nil { log.Errorf("panic in task: %v", r) }
			}()
			p.taskHandler(p.pollingCtx, task)
		}()
	}
}

func (p *Poller) Shutdown(ctx context.Context) error {
	p.cancel()
	p.wg.Wait()
	return nil
}

func (p *Poller) fetchTask(ctx context.Context) (*runnerv1.Task, bool) {
	reqCtx, cancel := context.WithTimeout(ctx, p.cfg.Runner.FetchTimeout)
	defer cancel()
	v := p.tasksVersion.Load()
	resp, err := p.client.FetchTask(reqCtx, connect.NewRequest(&runnerv1.FetchTaskRequest{TasksVersion: v}))
	if err != nil {
		if ctx.Err() == nil { log.WithError(err).Error("failed to fetch task") }
		return nil, false
	}
	task := resp.Msg.GetTask()
	if task != nil { p.tasksVersion.Store(task.Id) }
	return task, task != nil
}

func (p *Poller) PollOnce() {
	task, ok := p.fetchTask(context.Background())
	if ok && task != nil { p.taskHandler(context.Background(), task) }
}
