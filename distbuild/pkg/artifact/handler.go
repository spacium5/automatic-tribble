//go:build !solution

package artifact

import (
	"errors"
	"net/http"

	"go.uber.org/zap"

	"gitlab.com/slon/shad-go/distbuild/pkg/build"
	"gitlab.com/slon/shad-go/distbuild/pkg/tarstream"
)

type Handler struct {
	log   *zap.Logger
	cache *Cache
}

func NewHandler(l *zap.Logger, c *Cache) *Handler {
	return &Handler{
		log:   l,
		cache: c,
	}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/artifact", h.handleArtifact)
}

func (h *Handler) handleArtifact(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	idText := r.URL.Query().Get("id")
	var id build.ID
	if err := id.UnmarshalText([]byte(idText)); err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	path, unlock, err := h.cache.Get(id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			http.Error(rw, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	defer unlock()

	if err := tarstream.Send(path, rw); err != nil {
		h.log.Warn("failed to send artifact", zap.String("artifact_id", id.String()), zap.Error(err))
	}
}
