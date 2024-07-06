package models

import (
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"io"
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

type ReadRequest struct {
	PageIndex  uint32 `json:"page_index"`
	Version    string `json:"version"`
	Hash       []byte `json:"hash,omitempty"`
	AcceptZstd bool   `json:"accept_zstd"`
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

type Version int64

func (v Version) String() string {
	bytes := make([]byte, 8)
	bytes = binary.BigEndian.AppendUint64(bytes, uint64(v))
	return string(bytes)
}

func NewVersionFromString(s string) (Version, error) {
	bytes := []byte(s)
	if len(bytes) != 8 {
		return 0, errors.New("invalid version string")
	}
	return Version(binary.BigEndian.Uint64(bytes)), nil
}

type Page interface {
	DataCompressed() (io.ReadCloser, error)
	DataUncompressed() (io.ReadCloser, error)
}

type VersionedPage struct {
	Page    Page
	Version Version
}

func (v *VersionedPage) DataCompressed() (io.ReadCloser, error) {
	return v.DataCompressed()
}

func (v *VersionedPage) DataUncompressed() (io.ReadCloser, error) {
	return v.DataUncompressed()
}

type Hash [32]byte
