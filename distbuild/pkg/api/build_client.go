//go:build !solution

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"go.uber.org/zap"

	"gitlab.com/slon/shad-go/distbuild/pkg/build"
)

type BuildClient struct {
	log        *zap.Logger
	endpoint   string
	httpClient *http.Client
}

func NewBuildClient(l *zap.Logger, endpoint string) *BuildClient {
	return &BuildClient{
		log:        l,
		endpoint:   strings.TrimRight(endpoint, "/"),
		httpClient: &http.Client{},
	}
}

func (c *BuildClient) StartBuild(ctx context.Context, request *BuildRequest) (*BuildStarted, StatusReader, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return nil, nil, err
	}

	url := c.endpoint + "/build"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	c.log.Debug("sending start build request")

	rsp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, nil, err
	}

	if rsp.StatusCode != http.StatusOK {
		defer rsp.Body.Close()
		return nil, nil, readResponseError(rsp)
	}

	decoder := json.NewDecoder(rsp.Body)

	var msg buildStreamMessage
	if err := decoder.Decode(&msg); err != nil {
		_ = rsp.Body.Close()
		return nil, nil, err
	}

	if msg.Started == nil {
		_ = rsp.Body.Close()
		return nil, nil, fmt.Errorf("invalid /build response: missing started message")
	}

	return msg.Started, &statusReader{
		body: rsp.Body,
		dec:  decoder,
	}, nil
}

func (c *BuildClient) SignalBuild(ctx context.Context, buildID build.ID, signal *SignalRequest) (*SignalResponse, error) {
	body, err := json.Marshal(signal)
	if err != nil {
		return nil, err
	}

	url := c.endpoint + "/signal?build_id=" + buildID.String()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	c.log.Debug("sending build signal", zap.String("build_id", buildID.String()))

	rsp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer rsp.Body.Close()

	if rsp.StatusCode != http.StatusOK {
		return nil, readResponseError(rsp)
	}

	var result SignalResponse
	if err := json.NewDecoder(rsp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

type statusReader struct {
	body io.ReadCloser
	dec  *json.Decoder
}

func (r *statusReader) Close() error {
	return r.body.Close()
}

func (r *statusReader) Next() (*StatusUpdate, error) {
	for {
		var msg buildStreamMessage
		if err := r.dec.Decode(&msg); err != nil {
			return nil, err
		}

		if msg.Update != nil {
			return msg.Update, nil
		}
	}
}

func readResponseError(rsp *http.Response) error {
	body, _ := io.ReadAll(rsp.Body)
	text := strings.TrimSpace(string(body))
	if text == "" {
		return fmt.Errorf("request failed: %s", rsp.Status)
	}
	return fmt.Errorf("request failed: %s: %s", rsp.Status, text)
}
