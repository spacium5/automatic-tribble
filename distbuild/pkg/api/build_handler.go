//go:build !solution

package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"

	"go.uber.org/zap"

	"gitlab.com/slon/shad-go/distbuild/pkg/build"
)

func NewBuildService(l *zap.Logger, s Service) *BuildHandler {
	return &BuildHandler{
		log:     l,
		service: s,
	}
}

type BuildHandler struct {
	log     *zap.Logger
	service Service
}

func (h *BuildHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/build", h.handleBuild)
	mux.HandleFunc("/signal", h.handleSignal)
}

type buildStreamMessage struct {
	Started *BuildStarted `json:"started,omitempty"`
	Update  *StatusUpdate `json:"update,omitempty"`
}

type streamStatusWriter struct {
	mu sync.Mutex

	enc *json.Encoder
	rc  *http.ResponseController

	started bool
}

func (w *streamStatusWriter) Started(rsp *BuildStarted) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.started = true
	if err := w.enc.Encode(buildStreamMessage{Started: rsp}); err != nil {
		return err
	}
	return w.rc.Flush()
}

func (w *streamStatusWriter) Updated(update *StatusUpdate) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.enc.Encode(buildStreamMessage{Update: update}); err != nil {
		return err
	}
	return w.rc.Flush()
}

func (h *BuildHandler) handleBuild(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	h.log.Debug("build request received")

	var req BuildRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		h.log.Warn("failed to decode build request", zap.Error(err))
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	rw.Header().Set("Content-Type", "application/json")

	w := &streamStatusWriter{
		enc: json.NewEncoder(rw),
		rc:  http.NewResponseController(rw),
	}

	if err := h.service.StartBuild(r.Context(), &req, w); err != nil {
		h.log.Warn("build failed", zap.Error(err))
		if w.started {
			_ = w.Updated(&StatusUpdate{BuildFailed: &BuildFailed{Error: err.Error()}})
			return
		}
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (h *BuildHandler) handleSignal(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	idText := r.URL.Query().Get("build_id")
	var id build.ID
	if err := id.UnmarshalText([]byte(idText)); err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	var req SignalRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		h.log.Warn("failed to decode signal request", zap.Error(err))
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	h.log.Debug("signal request received", zap.String("build_id", id.String()))

	rsp, err := h.service.SignalBuild(r.Context(), id, &req)
	if err != nil {
		h.log.Warn("signal failed", zap.Error(err))
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	rw.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(rw).Encode(rsp); err != nil {
		h.log.Warn("failed to encode signal response", zap.Error(err))
	}
}
