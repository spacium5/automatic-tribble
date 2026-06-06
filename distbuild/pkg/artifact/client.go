//go:build !solution

package artifact

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"gitlab.com/slon/shad-go/distbuild/pkg/build"
	"gitlab.com/slon/shad-go/distbuild/pkg/tarstream"
)

// Download artifact from remote cache into local cache.
func Download(ctx context.Context, endpoint string, c *Cache, artifactID build.ID) error {
	for {
		dir, commit, abort, err := c.Create(artifactID)
		if err != nil {
			if errors.Is(err, ErrExists) {
				return nil
			}

			// Another goroutine may be writing this artifact already.
			if errors.Is(err, ErrWriteLocked) {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Millisecond * 10):
					continue
				}
			}
			return err
		}

		req, err := http.NewRequestWithContext(
			ctx,
			http.MethodGet,
			strings.TrimRight(endpoint, "/")+"/artifact?id="+artifactID.String(),
			nil,
		)
		if err != nil {
			_ = abort()
			return err
		}

		rsp, err := http.DefaultClient.Do(req)
		if err != nil {
			_ = abort()
			return err
		}

		if rsp.StatusCode != http.StatusOK {
			_ = rsp.Body.Close()
			_ = abort()
			return fmt.Errorf("artifact download failed: %s", rsp.Status)
		}

		receiveErr := tarstream.Receive(dir, rsp.Body)
		closeErr := rsp.Body.Close()
		if receiveErr != nil {
			_ = abort()
			return receiveErr
		}
		if closeErr != nil {
			_ = abort()
			return closeErr
		}

		if err := commit(); err != nil {
			return err
		}

		return nil
	}
}
