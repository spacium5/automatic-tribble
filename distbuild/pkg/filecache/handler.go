//go:build !solution

package filecache

import (
	"errors"
	"io"
	"net/http"
	"os"

	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"

	"gitlab.com/slon/shad-go/distbuild/pkg/build"
)

type Handler struct {
	log   *zap.Logger
	cache *Cache

	sf singleflight.Group
}

func NewHandler(l *zap.Logger, cache *Cache) *Handler {
	return &Handler{
		log:   l,
		cache: cache,
	}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/file", h.handleFile)
}

func (h *Handler) handleFile(rw http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.handleGetFile(rw, r)
	case http.MethodPut:
		h.handlePutFile(rw, r)
	default:
		http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *Handler) parseID(r *http.Request) (build.ID, error) {
	idText := r.URL.Query().Get("id")
	var id build.ID
	if err := id.UnmarshalText([]byte(idText)); err != nil {
		return build.ID{}, err
	}
	return id, nil
}

func (h *Handler) handleGetFile(rw http.ResponseWriter, r *http.Request) {
	id, err := h.parseID(r)
	if err != nil {
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

	f, err := os.Open(path)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	defer f.Close()

	if _, err := io.Copy(rw, f); err != nil {
		h.log.Warn("failed to send file", zap.String("file_id", id.String()), zap.Error(err))
	}
}

func (h *Handler) handlePutFile(rw http.ResponseWriter, r *http.Request) {
	id, err := h.parseID(r)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	_, err, _ = h.sf.Do(id.String(), func() (interface{}, error) {
		w, abort, err := h.cache.Write(id)
		if err != nil {
			// Repeated upload of the same file is expected.
			if errors.Is(err, ErrExists) {
				return nil, nil
			}
			return nil, err
		}

		if _, err := io.Copy(w, r.Body); err != nil {
			_ = abort()
			return nil, err
		}

		if err := w.Close(); err != nil {
			return nil, err
		}

		return nil, nil
	})
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
}
