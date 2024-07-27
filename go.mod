module s3qlite

go 1.22

toolchain go1.22.4

replace github.com/psanford/sqlite3vfs => github.com/bwarminski/sqlite3vfs v0.0.4

replace github.com/thomasjungblut/go-sstables/v2 => github.com/bwarminski/go-sstables/v2 v2.0.2

require (
	github.com/google/flatbuffers v24.3.25+incompatible
	github.com/huandu/skiplist v1.2.0
	github.com/psanford/sqlite3vfs v0.0.0-00010101000000-000000000000
	github.com/rs/zerolog v1.33.0
	github.com/stretchr/testify v1.8.1
	go.etcd.io/bbolt v1.3.10
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/sys v0.12.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
