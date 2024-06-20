package services

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"s3qlite/internal/models"
)

const IntervalScanSize = 100
const IntervalMaxSize = 1000

type StatService struct {
	*models.CommonDependencies
	ReadOnly bool
}

func NewStatService(deps *models.CommonDependencies, readOnly bool) *StatService {
	return &StatService{
		CommonDependencies: deps,
		ReadOnly:           readOnly,
	}
}

func (s *StatService) Stat(ctx context.Context, namespace string, fromVersion string, lockOwner string) (*models.StatResponse, error) {

	nsmd := models.NamespaceMetadata{}
	nsmdResp, err := s.KV.Get(ctx, namespace+"/nsmd")
	if err != nil {
		return nil, err
	}
	if nsmdResp.Count == 0 {
		return nil, errors.New("namespace metadata not found")
	}

	err = json.Unmarshal(nsmdResp.Kvs[0].Value, &nsmd)
	if err != nil {
		return nil, err
	}

	rev := nsmdResp.Header.Revision
	var version *string
	if nsmd.Lock != nil {
		if lockOwner == "" {
			// Concurrent read-only transaction, return the latest snapshot
			version = &nsmd.Lock.SnapshotVersion
		} else {
			if nsmd.Lock.Owner != lockOwner {
				return nil, models.NewGoneErr(errors.New("you no longer own the lock"))
			}
			if nsmd.Lock.RollingBack {
				return nil, models.NewGoneErr(errors.New("rolling back"))
			}
		}
	} else {
		if lockOwner != "" {
			return nil, models.NewGoneErr(errors.New("you do not own the lock"))
		}
	}

	if version == nil {
		lwvResp, err := s.KV.Get(ctx, namespace+"/lwv", clientv3.WithRev(rev))
		if err != nil {
			return nil, err
		}
		if lwvResp.Count == 0 {
			version = &models.ZeroLwv
		} else {
			var v int64
			buf := bytes.NewBuffer(lwvResp.Kvs[0].Value)
			err := binary.Read(buf, binary.BigEndian, &v)
			if err != nil {
				return nil, err
			}
			s := fmt.Sprintf("%019d", v)
			version = &s
		}
	}

	var interval []uint32
	if fromVersion != "" {
		interval, err = s.read_interval(ctx, namespace, fromVersion, *version, rev, true)
		if err != nil {
			return nil, err
		}
	}
	return &models.StatResponse{
		Version:  *version,
		Metadata: "",
		ReadOnly: s.ReadOnly,
		Interval: interval,
	}, nil

}

func (s *StatService) read_interval(ctx context.Context, namespace string, fromVersion string, toVersion string, revision int64, inclusive bool) ([]uint32, error) {
	if fromVersion > toVersion {
		return nil, nil
	}

	if fromVersion == toVersion {
		return []uint32{}, nil
	}

	fromKey := namespace + "/c/" + fromVersion
	toKey := namespace + "/c/" + toVersion

	resp, err := s.KV.Get(
		ctx,
		fromKey,
		clientv3.WithRev(revision),
		clientv3.WithRange(clientv3.GetPrefixRangeEnd(toKey)),
		clientv3.WithLimit(IntervalScanSize),
	)

	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) < 2 {
		return nil, nil
	}

	if string(resp.Kvs[0].Key) != fromKey || string(resp.Kvs[len(resp.Kvs)-1].Key) != toKey {
		return nil, nil
	}

	kvs := resp.Kvs[1:]
	set := make(map[uint32]interface{})
	for _, kv := range kvs {
		if len(kv.Value) < 1 {
			return nil, nil
		}
		ty := kv.Value[0]
		value := kv.Value[1:]
		switch ty {
		case 0:
			if len(value)%4 != 0 {
				return nil, nil
			}
			for len(value) > 0 {
				v := binary.BigEndian.Uint32(value)
				set[v] = nil
				if len(set) > IntervalMaxSize {
					return nil, nil
				}
				value = value[4:]
			}
		case 1:
			// infinite
			return nil, nil
		default:
			return nil, nil
		}
	}
	keys := make([]uint32, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	return keys, nil
}
