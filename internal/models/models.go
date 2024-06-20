package models

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var ZeroLwv = fmt.Sprintf("%019d", 0)

type StatResponse struct {
	Version  string   `json:"version"`
	Metadata string   `json:"metadata"`
	ReadOnly bool     `json:"read_only"`
	Interval []uint32 `json:"interval,omitempty"`
}

type NamespaceMetadata struct {
	Lock *NamespaceLock `json:"lock,omitempty"`
}

type NamespaceLock struct {
	SnapshotVersion string `json:"snapshot_version"`
	Owner           string `json:"owner"`
	Nonce           string `json:"nonce"`
	RollingBack     bool   `json:"rolling_back"`
}

// Define the GoneErr type
type GoneErr struct {
	Err error
}

// Implement the Error method for GoneErr
func (e *GoneErr) Error() string {
	return e.Err.Error()
}

// Implement a function to wrap an error with GoneErr
func NewGoneErr(err error) error {
	return &GoneErr{
		Err: errors.Wrap(err, "resource gone"),
	}
}

type CommonDependencies struct {
	KV     clientv3.KV
	Logger zerolog.Logger
}
