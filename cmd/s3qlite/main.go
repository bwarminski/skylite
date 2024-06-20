package main

import (
	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"net/http"
	"os"
	"s3qlite/internal/models"
	"s3qlite/internal/server"
	"s3qlite/internal/services"
)

func main() {
	output := zerolog.ConsoleWriter{Out: os.Stderr}
	log := zerolog.New(output).With().Timestamp().Logger()

	etcd, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"http://localhost:2379"},
	})
	if err != nil {
		log.Fatal().Err(err).Msg("error connecting to etcd")
	}
	defer func(etcd *clientv3.Client) {
		err := etcd.Close()
		if err != nil {
			log.Error().Err(err).Msg("error closing etcd client")
		}
	}(etcd)

	deps := models.CommonDependencies{
		KV:     etcd.KV,
		Logger: log,
	}

	s := server.New(&server.Dependencies{
		CommonDependencies: &deps,
		StatService:        services.NewStatService(&deps, false),
	})
	err = http.ListenAndServe(":8080", s)
	if err != nil {
		log.Fatal().Err(err).Msg("error starting server")
	}
}
