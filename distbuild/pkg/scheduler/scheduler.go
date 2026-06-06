//go:build !solution

package scheduler

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"gitlab.com/slon/shad-go/distbuild/pkg/api"
	"gitlab.com/slon/shad-go/distbuild/pkg/build"
)

type PendingJob struct {
	Job      *api.JobSpec
	Finished chan struct{}
	Result   *api.JobResult
}

type Config struct {
	CacheTimeout time.Duration
	DepsTimeout  time.Duration
}

type Scheduler struct {
	log       *zap.Logger
	config    Config
	timeAfter func(d time.Duration) <-chan time.Time

	mu         sync.Mutex
	stopped    bool
	queue      []*PendingJob
	pending    map[build.ID]*PendingJob
	completed  map[build.ID]*api.JobResult
	artifacts  map[build.ID]map[api.WorkerID]struct{}
	queueEvent chan struct{}
}

func NewScheduler(l *zap.Logger, config Config, timeAfter func(d time.Duration) <-chan time.Time) *Scheduler {
	return &Scheduler{
		log:        l,
		config:     config,
		timeAfter:  timeAfter,
		pending:    make(map[build.ID]*PendingJob),
		completed:  make(map[build.ID]*api.JobResult),
		artifacts:  make(map[build.ID]map[api.WorkerID]struct{}),
		queueEvent: make(chan struct{}, 1),
	}
}

func (c *Scheduler) LocateArtifact(id build.ID) (api.WorkerID, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for w := range c.artifacts[id] {
		return w, true
	}
	return "", false
}

func (c *Scheduler) OnJobComplete(workerID api.WorkerID, jobID build.ID, res *api.JobResult) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.artifacts[jobID]; !ok {
		c.artifacts[jobID] = make(map[api.WorkerID]struct{})
	}
	c.artifacts[jobID][workerID] = struct{}{}

	pending, ok := c.pending[jobID]
	if !ok {
		if res != nil {
			c.completed[jobID] = res
		}
		return false
	}
	if res == nil {
		return false
	}

	if pending.Result != nil {
		return true
	}

	pending.Result = res
	delete(c.pending, jobID)
	c.completed[jobID] = res
	close(pending.Finished)
	return true
}

func (c *Scheduler) ScheduleJob(job *api.JobSpec) *PendingJob {
	c.mu.Lock()
	defer c.mu.Unlock()

	if result, ok := c.completed[job.ID]; ok {
		p := &PendingJob{
			Job:      job,
			Finished: make(chan struct{}),
			Result:   result,
		}
		close(p.Finished)
		return p
	}

	if pending, ok := c.pending[job.ID]; ok {
		return pending
	}

	p := &PendingJob{
		Job:      job,
		Finished: make(chan struct{}),
	}

	c.pending[job.ID] = p
	c.queue = append(c.queue, p)
	select {
	case c.queueEvent <- struct{}{}:
	default:
	}

	return p
}

func (c *Scheduler) PickJob(ctx context.Context, workerID api.WorkerID) *PendingJob {
	for {
		c.mu.Lock()
		if c.stopped {
			c.mu.Unlock()
			return nil
		}

		if len(c.queue) > 0 {
			job := c.queue[0]
			c.queue = c.queue[1:]
			c.mu.Unlock()
			return job
		}
		c.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil
		case <-c.queueEvent:
		}
	}
}

func (c *Scheduler) Stop() {
	c.mu.Lock()
	c.stopped = true
	c.mu.Unlock()

	select {
	case c.queueEvent <- struct{}{}:
	default:
	}
}
