//go:build !solution

package dist

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"

	"gitlab.com/slon/shad-go/distbuild/pkg/api"
	"gitlab.com/slon/shad-go/distbuild/pkg/build"
	"gitlab.com/slon/shad-go/distbuild/pkg/filecache"
	"gitlab.com/slon/shad-go/distbuild/pkg/scheduler"
)

type Coordinator struct {
	log       *zap.Logger
	fileCache *filecache.Cache
	sched     *scheduler.Scheduler

	mux *http.ServeMux

	buildsMu sync.Mutex
	builds   map[build.ID]*runningBuild
}

func NewCoordinator(
	log *zap.Logger,
	fileCache *filecache.Cache,
) *Coordinator {
	c := &Coordinator{
		log:       log,
		fileCache: fileCache,
		sched:     scheduler.NewScheduler(log.Named("scheduler"), scheduler.Config{}, time.After),
		builds:    make(map[build.ID]*runningBuild),
		mux:       http.NewServeMux(),
	}

	api.NewBuildService(log.Named("build"), c).Register(c.mux)
	api.NewHeartbeatHandler(log.Named("heartbeat"), c).Register(c.mux)
	filecache.NewHandler(log.Named("filecache"), fileCache).Register(c.mux)

	return c
}

func (c *Coordinator) Stop() {
	c.sched.Stop()
}

func (c *Coordinator) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.mux.ServeHTTP(w, r)
}

type runningBuild struct {
	id         build.ID
	graph      build.Graph
	uploadDone chan struct{}
	uploadOnce sync.Once
}

func (c *Coordinator) StartBuild(ctx context.Context, request *api.BuildRequest, w api.StatusWriter) error {
	b := &runningBuild{
		id:         build.NewID(),
		graph:      request.Graph,
		uploadDone: make(chan struct{}),
	}

	c.buildsMu.Lock()
	c.builds[b.id] = b
	c.buildsMu.Unlock()
	defer func() {
		c.buildsMu.Lock()
		delete(c.builds, b.id)
		c.buildsMu.Unlock()
	}()

	missing, err := c.findMissingFiles(request.Graph.SourceFiles)
	if err != nil {
		return err
	}

	if err := w.Started(&api.BuildStarted{ID: b.id, MissingFiles: missing}); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.uploadDone:
	}

	sortedJobs := build.TopSort(request.Graph.Jobs)
	for _, job := range sortedJobs {
		spec, err := c.buildJobSpec(request.Graph, job)
		if err != nil {
			return err
		}

		pending := c.sched.ScheduleJob(spec)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-pending.Finished:
		}

		if pending.Result == nil {
			return fmt.Errorf("job finished without result: %s", job.ID)
		}

		if err := w.Updated(&api.StatusUpdate{JobFinished: pending.Result}); err != nil {
			return err
		}

		if pending.Result.Error != nil {
			return fmt.Errorf("job %s failed: %s", job.Name, *pending.Result.Error)
		}
		if pending.Result.ExitCode != 0 {
			return fmt.Errorf("job %s failed: exit code %d", job.Name, pending.Result.ExitCode)
		}
	}

	return w.Updated(&api.StatusUpdate{BuildFinished: &api.BuildFinished{}})
}

func (c *Coordinator) SignalBuild(ctx context.Context, buildID build.ID, signal *api.SignalRequest) (*api.SignalResponse, error) {
	c.buildsMu.Lock()
	b, ok := c.builds[buildID]
	c.buildsMu.Unlock()
	if !ok {
		return nil, fmt.Errorf("build not found: %s", buildID)
	}

	if signal.UploadDone != nil {
		b.uploadOnce.Do(func() {
			close(b.uploadDone)
		})
	}

	return &api.SignalResponse{}, nil
}

func (c *Coordinator) Heartbeat(ctx context.Context, req *api.HeartbeatRequest) (*api.HeartbeatResponse, error) {
	for _, finished := range req.FinishedJob {
		result := finished
		c.sched.OnJobComplete(req.WorkerID, finished.ID, &result)
	}

	for _, artifactID := range req.AddedArtifacts {
		c.sched.OnJobComplete(req.WorkerID, artifactID, nil)
	}

	jobsToRun := make(map[build.ID]api.JobSpec)
	for i := 0; i < req.FreeSlots; i++ {
		pickCtx, cancel := context.WithTimeout(ctx, time.Millisecond*5)
		pending := c.sched.PickJob(pickCtx, req.WorkerID)
		cancel()
		if pending == nil {
			break
		}
		jobsToRun[pending.Job.ID] = *pending.Job
	}

	return &api.HeartbeatResponse{JobsToRun: jobsToRun}, nil
}

func (c *Coordinator) findMissingFiles(files map[build.ID]string) ([]build.ID, error) {
	var missing []build.ID
	for id := range files {
		_, unlock, err := c.fileCache.Get(id)
		if err == nil {
			unlock()
			continue
		}
		if errors.Is(err, filecache.ErrNotFound) {
			missing = append(missing, id)
			continue
		}
		return nil, err
	}
	return missing, nil
}

func (c *Coordinator) buildJobSpec(graph build.Graph, job build.Job) (*api.JobSpec, error) {
	inputIDByPath := make(map[string]build.ID, len(graph.SourceFiles))
	for id, path := range graph.SourceFiles {
		inputIDByPath[path] = id
	}

	sourceFiles := make(map[build.ID]string)
	for _, input := range job.Inputs {
		id, ok := inputIDByPath[input]
		if !ok {
			return nil, fmt.Errorf("job %s references unknown input file %q", job.Name, input)
		}
		sourceFiles[id] = input
	}

	artifacts := make(map[build.ID]api.WorkerID)
	for _, dep := range job.Deps {
		workerID, ok := c.sched.LocateArtifact(dep)
		if !ok {
			return nil, fmt.Errorf("artifact for dep %s is not available", dep)
		}
		artifacts[dep] = workerID
	}

	return &api.JobSpec{
		SourceFiles: sourceFiles,
		Artifacts:   artifacts,
		Job:         job,
	}, nil
}
