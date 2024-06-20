package server

import (
	"net/http"
	"s3qlite/internal/models"
	"s3qlite/internal/server/handlers"
	"s3qlite/internal/services"
)

type Dependencies struct {
	*models.CommonDependencies
	StatService *services.StatService
}

func New(dependencies *Dependencies) *http.ServeMux {
	mux := http.NewServeMux()
	middleware := handlers.NewNamespaceMiddleware()
	statHandler := handlers.NewStatHandler(logger, NewStatSer)
	mux.Handle("/stat", middleware.Middleware(handlers.StatHandler{}.ServeHTTP))
	return mux
}
