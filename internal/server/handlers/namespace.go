package handlers

import (
	"errors"
	"net/http"
	"s3qlite/internal/models"
)

type NamespacedHandler func(http.ResponseWriter, *http.Request, string)

type NamespaceMiddleware struct {
}

func NewNamespaceMiddleware() NamespaceMiddleware {
	return NamespaceMiddleware{}
}

func (n *NamespaceMiddleware) Middleware(next NamespacedHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Get the namespace from the request
		namespace := r.Header.Get("x-namespace-key")
		if namespace == "" {
			http.Error(w, "invalid x-namespace-key", http.StatusBadRequest)
			return
		}

		// Call the next handler with the namespace
		next(w, r, namespace)
	})
}

func handleError(w http.ResponseWriter, err error) {
	var goneErr *models.GoneErr
	if errors.As(err, &goneErr) {
		http.Error(w, "", http.StatusGone)
		return
	}
	http.Error(w, "", http.StatusInternalServerError)
}
