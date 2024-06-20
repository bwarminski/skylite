package handlers

import (
	"encoding/json"
	"net/http"
	"s3qlite/internal/models"
	"s3qlite/internal/services"
)

type StatHandler struct {
	*models.CommonDependencies
	Service *services.StatService
}

func NewStatHandler(deps *models.CommonDependencies, service *services.StatService) StatHandler {
	return StatHandler{
		deps,
		service,
	}
}

func (s *StatHandler) ServeHTTP(w http.ResponseWriter, r *http.Request, namespace string) {
	fromVersion := r.Form.Get("from_version")
	lockOwner := r.Form.Get("lock_owner")

	s.Logger.Debug().Str("fromVersion", fromVersion).Str("lockOwner", lockOwner).Str("namespace", namespace).Msg("Stat called")

	stat, err := s.Service.Stat(r.Context(), namespace, fromVersion, lockOwner)
	if err != nil {
		s.Logger.Error().Err(err).Msg("Error in Stat service")
		handleError(w, err)
		return
	}
	body, err := json.Marshal(stat)
	if err != nil {
		s.Logger.Error().Err(err).Msg("Error marshalling stat to JSON")
		handleError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}
