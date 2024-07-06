package handlers

import (
	"io"
	"net/http"
	"s3qlite/internal/models"
)

const MaxMessageSize = 40 * 1024

type ReadHandler struct {
	*models.CommonDependencies
}

func NewReadHandler(commonDependencies *models.CommonDependencies) *ReadHandler {
	return &ReadHandler{
		CommonDependencies: commonDependencies,
	}
}

func (h *ReadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request, namespace string) {
	reader := NewLengthDelimitedFrameReader(r.Body)
	defer func(reader io.ReadCloser) {
		err := reader.Close()
		if err != nil {
			h.Logger.Error().Err(err).Msg("failed to close reader")
		}
	}(reader)

	buf := make([]byte, MaxMessageSize)
	for {
		n, err := reader.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			handleError(w, err)
			return
		}
		if n == 0 {
			break
		}

		// process the message

	}
	panic("implement me")
}
