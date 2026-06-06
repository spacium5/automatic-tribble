//go:build !solution

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"go.uber.org/zap"
)

type HeartbeatClient struct {
	log        *zap.Logger
	endpoint   string
	httpClient *http.Client
}

func NewHeartbeatClient(l *zap.Logger, endpoint string) *HeartbeatClient {
	return &HeartbeatClient{
		log:        l,
		endpoint:   strings.TrimRight(endpoint, "/"),
		httpClient: &http.Client{},
	}
}

func (c *HeartbeatClient) Heartbeat(ctx context.Context, req *HeartbeatRequest) (*HeartbeatResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/heartbeat", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	rsp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer rsp.Body.Close()

	if rsp.StatusCode != http.StatusOK {
		return nil, readResponseError(rsp)
	}

	var response HeartbeatResponse
	if err := json.NewDecoder(rsp.Body).Decode(&response); err != nil {
		return nil, err
	}
	return &response, nil
}
