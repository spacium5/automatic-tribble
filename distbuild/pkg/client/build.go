//go:build !solution

package client

import (
	"context"
	"fmt"
	"io"
	"path/filepath"

	"go.uber.org/zap"

	"gitlab.com/slon/shad-go/distbuild/pkg/api"
	"gitlab.com/slon/shad-go/distbuild/pkg/build"
	"gitlab.com/slon/shad-go/distbuild/pkg/filecache"
)

type Client struct {
	log       *zap.Logger
	apiClient *api.BuildClient
	files     *filecache.Client
	sourceDir string
}

func NewClient(
	l *zap.Logger,
	apiEndpoint string,
	sourceDir string,
) *Client {
	return &Client{
		log:       l,
		apiClient: api.NewBuildClient(l.Named("api"), apiEndpoint),
		files:     filecache.NewClient(l.Named("filecache"), apiEndpoint),
		sourceDir: sourceDir,
	}
}

type BuildListener interface {
	OnJobStdout(jobID build.ID, stdout []byte) error
	OnJobStderr(jobID build.ID, stderr []byte) error

	OnJobFinished(jobID build.ID) error
	OnJobFailed(jobID build.ID, code int, error string) error
}

func (c *Client) Build(ctx context.Context, graph build.Graph, lsn BuildListener) error {
	started, reader, err := c.apiClient.StartBuild(ctx, &api.BuildRequest{Graph: graph})
	if err != nil {
		return err
	}
	defer reader.Close()

	for _, fileID := range started.MissingFiles {
		relPath, ok := graph.SourceFiles[fileID]
		if !ok {
			return fmt.Errorf("missing source file mapping for %s", fileID)
		}

		absPath := filepath.Join(c.sourceDir, relPath)
		if err := c.files.Upload(ctx, fileID, absPath); err != nil {
			return err
		}
	}

	if _, err := c.apiClient.SignalBuild(ctx, started.ID, &api.SignalRequest{UploadDone: &api.UploadDone{}}); err != nil {
		return err
	}

	for {
		update, err := reader.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		if update.JobFinished != nil {
			res := update.JobFinished
			if len(res.Stdout) > 0 {
				if err := lsn.OnJobStdout(res.ID, res.Stdout); err != nil {
					return err
				}
			}
			if len(res.Stderr) > 0 {
				if err := lsn.OnJobStderr(res.ID, res.Stderr); err != nil {
					return err
				}
			}

			if res.Error != nil || res.ExitCode != 0 {
				errorText := ""
				if res.Error != nil {
					errorText = *res.Error
				}
				if err := lsn.OnJobFailed(res.ID, res.ExitCode, errorText); err != nil {
					return err
				}
			} else {
				if err := lsn.OnJobFinished(res.ID); err != nil {
					return err
				}
			}
		}

		if update.BuildFailed != nil {
			return fmt.Errorf("build failed: %s", update.BuildFailed.Error)
		}

		if update.BuildFinished != nil {
			return nil
		}
	}
}
