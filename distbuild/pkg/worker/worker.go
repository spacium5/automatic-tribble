//go:build !solution

package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"

	"gitlab.com/slon/shad-go/distbuild/pkg/api"
	"gitlab.com/slon/shad-go/distbuild/pkg/artifact"
	"gitlab.com/slon/shad-go/distbuild/pkg/build"
	"gitlab.com/slon/shad-go/distbuild/pkg/filecache"
)

type Worker struct {
	workerID             api.WorkerID
	coordinatorEndpoint  string
	log                  *zap.Logger
	fileCache            *filecache.Cache
	artifacts            *artifact.Cache
	heartbeatClient      *api.HeartbeatClient
	fileClient           *filecache.Client
	mux                  *http.ServeMux
	maxSlots             int
	runningMu            sync.Mutex
	running              map[build.ID]struct{}
	finishedCh           chan api.JobResult
	addedArtifactCh      chan build.ID
	jobResultMu          sync.Mutex
	jobResultsByArtifact map[build.ID]*api.JobResult
}

func New(
	workerID api.WorkerID,
	coordinatorEndpoint string,
	log *zap.Logger,
	fileCache *filecache.Cache,
	artifacts *artifact.Cache,
) *Worker {
	w := &Worker{
		workerID:             workerID,
		coordinatorEndpoint:  coordinatorEndpoint,
		log:                  log,
		fileCache:            fileCache,
		artifacts:            artifacts,
		heartbeatClient:      api.NewHeartbeatClient(log.Named("heartbeat"), coordinatorEndpoint),
		fileClient:           filecache.NewClient(log.Named("files"), coordinatorEndpoint),
		mux:                  http.NewServeMux(),
		maxSlots:             1,
		running:              make(map[build.ID]struct{}),
		finishedCh:           make(chan api.JobResult, 128),
		addedArtifactCh:      make(chan build.ID, 128),
		jobResultsByArtifact: make(map[build.ID]*api.JobResult),
	}

	artifact.NewHandler(log.Named("artifact"), artifacts).Register(w.mux)

	return w
}

func (w *Worker) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	w.mux.ServeHTTP(rw, r)
}

func (w *Worker) Run(ctx context.Context) error {
	for {
		finished, added := w.collectUpdates()
		runningIDs, freeSlots := w.runningState()

		req := &api.HeartbeatRequest{
			WorkerID:       w.workerID,
			RunningJobs:    runningIDs,
			FreeSlots:      freeSlots,
			FinishedJob:    finished,
			AddedArtifacts: added,
		}

		rsp, err := w.heartbeatClient.Heartbeat(ctx, req)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			w.log.Warn("heartbeat failed", zap.Error(err))

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Millisecond * 50):
			}
			continue
		}

		for _, job := range rsp.JobsToRun {
			job := job
			w.startJob(ctx, &job)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond * 20):
		}
	}
}

func (w *Worker) collectUpdates() ([]api.JobResult, []build.ID) {
	var finished []api.JobResult
	for {
		select {
		case result := <-w.finishedCh:
			finished = append(finished, result)
		default:
			goto collectArtifacts
		}
	}

collectArtifacts:
	var artifacts []build.ID
	for {
		select {
		case id := <-w.addedArtifactCh:
			artifacts = append(artifacts, id)
		default:
			return finished, artifacts
		}
	}
}

func (w *Worker) runningState() ([]build.ID, int) {
	w.runningMu.Lock()
	defer w.runningMu.Unlock()

	running := make([]build.ID, 0, len(w.running))
	for id := range w.running {
		running = append(running, id)
	}

	return running, w.maxSlots - len(w.running)
}

func (w *Worker) startJob(ctx context.Context, job *api.JobSpec) {
	w.runningMu.Lock()
	if len(w.running) >= w.maxSlots {
		w.runningMu.Unlock()
		return
	}

	if _, exists := w.running[job.ID]; exists {
		w.runningMu.Unlock()
		return
	}

	w.running[job.ID] = struct{}{}
	w.runningMu.Unlock()

	go func() {
		defer func() {
			w.runningMu.Lock()
			delete(w.running, job.ID)
			w.runningMu.Unlock()
		}()

		res, artifactAdded := w.runJob(ctx, job)

		select {
		case w.finishedCh <- *res:
		default:
			w.log.Warn("dropping finished job update", zap.String("job_id", job.ID.String()))
		}

		if artifactAdded {
			select {
			case w.addedArtifactCh <- job.ID:
			default:
				w.log.Warn("dropping artifact update", zap.String("job_id", job.ID.String()))
			}
		}
	}()
}

func (w *Worker) runJob(ctx context.Context, job *api.JobSpec) (*api.JobResult, bool) {
	result := &api.JobResult{ID: job.ID}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	artifactDir, commit, abort, err := w.artifacts.Create(job.ID)
	if err != nil {
		if errors.Is(err, artifact.ErrExists) {
			if cached := w.getJobResult(job.ID); cached != nil {
				return cached, true
			}
			return result, true
		}
		msg := err.Error()
		result.Error = &msg
		result.ExitCode = -1
		return result, false
	}

	abortArtifact := true
	defer func() {
		if abortArtifact {
			_ = abort()
		}
	}()

	deps, unlockDeps, err := w.ensureArtifacts(ctx, job.Artifacts)
	if err != nil {
		msg := err.Error()
		result.Error = &msg
		result.ExitCode = -1
		return result, false
	}
	defer unlockDeps()

	sourceDir, err := os.MkdirTemp("", "distbuild-source-*")
	if err != nil {
		msg := err.Error()
		result.Error = &msg
		result.ExitCode = -1
		return result, false
	}
	defer os.RemoveAll(sourceDir)

	if err := w.ensureSourceFiles(ctx, sourceDir, job.SourceFiles); err != nil {
		msg := err.Error()
		result.Error = &msg
		result.ExitCode = -1
		return result, false
	}

	for _, cmd := range job.Cmds {
		rendered, err := cmd.Render(build.JobContext{
			SourceDir: sourceDir,
			OutputDir: artifactDir,
			Deps:      deps,
		})
		if err != nil {
			msg := err.Error()
			result.Error = &msg
			result.ExitCode = -1
			break
		}

		if err := runRenderedCommand(ctx, rendered, &stdout, &stderr); err != nil {
			result.ExitCode = -1

			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				result.ExitCode = exitErr.ExitCode()
			}

			msg := err.Error()
			result.Error = &msg
			break
		}
	}

	result.Stdout = append(result.Stdout, stdout.Bytes()...)
	result.Stderr = append(result.Stderr, stderr.Bytes()...)

	if result.Error != nil {
		return result, false
	}

	if err := commit(); err != nil {
		msg := err.Error()
		result.Error = &msg
		result.ExitCode = -1
		return result, false
	}
	abortArtifact = false

	w.storeJobResult(result)
	return result, true
}

func runRenderedCommand(ctx context.Context, cmd *build.Cmd, stdout, stderr *bytes.Buffer) error {
	if cmd.CatOutput != "" {
		if err := os.MkdirAll(filepath.Dir(cmd.CatOutput), 0o777); err != nil {
			return err
		}
		return os.WriteFile(cmd.CatOutput, []byte(cmd.CatTemplate), 0o666)
	}

	if len(cmd.Exec) == 0 {
		return nil
	}

	execCmd := exec.CommandContext(ctx, cmd.Exec[0], cmd.Exec[1:]...)
	if len(cmd.Environ) > 0 {
		execCmd.Env = append(os.Environ(), cmd.Environ...)
	}
	if cmd.WorkingDirectory != "" {
		execCmd.Dir = cmd.WorkingDirectory
	}

	execCmd.Stdout = stdout
	execCmd.Stderr = stderr
	return execCmd.Run()
}

func (w *Worker) ensureArtifacts(ctx context.Context, artifactMap map[build.ID]api.WorkerID) (map[build.ID]string, func(), error) {
	deps := make(map[build.ID]string, len(artifactMap))
	var unlocks []func()

	unlockAll := func() {
		for _, unlock := range unlocks {
			unlock()
		}
	}

	for depID, workerID := range artifactMap {
		path, unlock, err := w.artifacts.Get(depID)
		if errors.Is(err, artifact.ErrNotFound) {
			if workerID == "" {
				unlockAll()
				return nil, func() {}, fmt.Errorf("missing endpoint for artifact %s", depID)
			}

			if err := artifact.Download(ctx, workerID.String(), w.artifacts, depID); err != nil {
				unlockAll()
				return nil, func() {}, err
			}

			path, unlock, err = w.artifacts.Get(depID)
		}
		if err != nil {
			unlockAll()
			return nil, func() {}, err
		}

		deps[depID] = path
		unlocks = append(unlocks, unlock)
	}

	return deps, unlockAll, nil
}

func (w *Worker) ensureSourceFiles(ctx context.Context, sourceDir string, files map[build.ID]string) error {
	for id, relPath := range files {
		cachePath, unlock, err := w.fileCache.Get(id)
		if errors.Is(err, filecache.ErrNotFound) {
			if err := w.fileClient.Download(ctx, w.fileCache, id); err != nil {
				return err
			}
			cachePath, unlock, err = w.fileCache.Get(id)
		}
		if err != nil {
			return err
		}

		dstPath := filepath.Join(sourceDir, relPath)
		if err := os.MkdirAll(filepath.Dir(dstPath), 0o777); err != nil {
			unlock()
			return err
		}

		if err := copyFile(cachePath, dstPath); err != nil {
			unlock()
			return err
		}
		unlock()
	}

	return nil
}

func copyFile(from, to string) error {
	src, err := os.Open(from)
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.OpenFile(to, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o666)
	if err != nil {
		return err
	}
	defer dst.Close()

	_, err = io.Copy(dst, src)
	return err
}

func (w *Worker) getJobResult(id build.ID) *api.JobResult {
	w.jobResultMu.Lock()
	defer w.jobResultMu.Unlock()

	r, ok := w.jobResultsByArtifact[id]
	if !ok {
		return nil
	}

	cp := *r
	cp.Stdout = append([]byte{}, r.Stdout...)
	cp.Stderr = append([]byte{}, r.Stderr...)
	if r.Error != nil {
		errText := *r.Error
		cp.Error = &errText
	}
	return &cp
}

func (w *Worker) storeJobResult(r *api.JobResult) {
	w.jobResultMu.Lock()
	defer w.jobResultMu.Unlock()

	cp := *r
	cp.Stdout = append([]byte{}, r.Stdout...)
	cp.Stderr = append([]byte{}, r.Stderr...)
	if r.Error != nil {
		errText := *r.Error
		cp.Error = &errText
	}
	w.jobResultsByArtifact[r.ID] = &cp
}
