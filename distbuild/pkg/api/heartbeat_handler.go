//go:build !solution

package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"go.uber.org/zap"
)

type HeartbeatHandler struct {
	log     *zap.Logger
	service HeartbeatService
}

func NewHeartbeatHandler(l *zap.Logger, s HeartbeatService) *HeartbeatHandler {
	return &HeartbeatHandler{
		log:     l,
		service: s,
	}
}

func (h *HeartbeatHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/heartbeat", h.handleHeartbeat)
}

func (h *HeartbeatHandler) handleHeartbeat(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req HeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		h.log.Warn("failed to decode heartbeat request", zap.Error(err))
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	rsp, err := h.service.Heartbeat(r.Context(), &req)
	if err != nil {
		h.log.Warn("heartbeat failed", zap.Error(err))
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	rw.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(rw).Encode(rsp); err != nil {
		h.log.Warn("failed to encode heartbeat response", zap.Error(err))
	}
}
