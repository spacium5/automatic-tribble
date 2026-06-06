//go:build !solution

package auth

import (
	"context"
	"errors"
	"net/http"
	"strings"
)

type User struct {
	Name  string
	Email string
}

type ctxKey int

const userKey ctxKey = 1

func ContextUser(ctx context.Context) (*User, bool) {
	u, ok := ctx.Value(userKey).(*User)
	return u, ok
}

var ErrInvalidToken = errors.New("invalid token")

type TokenChecker interface {
	CheckToken(ctx context.Context, token string) (*User, error)
}

func CheckAuth(checker TokenChecker) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			header := r.Header.Get("Authorization")
			const prefix = "Bearer "
			if !strings.HasPrefix(header, prefix) {
				http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
				return
			}

			token := strings.TrimPrefix(header, prefix)
			user, err := checker.CheckToken(r.Context(), token)
			if err != nil {
				if errors.Is(err, ErrInvalidToken) {
					http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
					return
				}
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				return
			}

			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userKey, user)))
		})
	}
}
