//go:build !solution

package filecache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"go.uber.org/zap"

	"gitlab.com/slon/shad-go/distbuild/pkg/build"
)

type Client struct {
	log      *zap.Logger
	endpoint string
	client   *http.Client
}

func NewClient(l *zap.Logger, endpoint string) *Client {
	return &Client{
		log:      l,
		endpoint: strings.TrimRight(endpoint, "/"),
		client:   &http.Client{},
	}
}

func (c *Client) Upload(ctx context.Context, id build.ID, localPath string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer f.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.endpoint+"/file?id="+id.String(), f)
	if err != nil {
		return err
	}

	rsp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer rsp.Body.Close()

	if rsp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(rsp.Body)
		return fmt.Errorf("upload failed: %s: %s", rsp.Status, strings.TrimSpace(string(body)))
	}

	return nil
}

func (c *Client) Download(ctx context.Context, localCache *Cache, id build.ID) error {
	w, abort, err := localCache.Write(id)
	if err != nil {
		if errors.Is(err, ErrExists) {
			return nil
		}
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint+"/file?id="+id.String(), nil)
	if err != nil {
		_ = abort()
		return err
	}

	rsp, err := c.client.Do(req)
	if err != nil {
		_ = abort()
		return err
	}

	if rsp.StatusCode != http.StatusOK {
		defer rsp.Body.Close()
		_ = abort()
		body, _ := io.ReadAll(rsp.Body)
		return fmt.Errorf("download failed: %s: %s", rsp.Status, strings.TrimSpace(string(body)))
	}

	_, copyErr := io.Copy(w, rsp.Body)
	closeBodyErr := rsp.Body.Close()
	if copyErr != nil {
		_ = abort()
		return copyErr
	}
	if closeBodyErr != nil {
		_ = abort()
		return closeBodyErr
	}

	if err := w.Close(); err != nil {
		return err
	}

	return nil
}
