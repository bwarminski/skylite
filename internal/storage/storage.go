package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"github.com/klauspost/compress/zstd"
	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"s3qlite/internal/models"
)

type PagePointer struct {
	Hash  models.Hash
	Base  *models.Hash
	Delta []byte
}

func (p PagePointer) MarshalBinary() []byte {
	b := make([]byte, 32)
	copy(b, p.Hash[:])
	if p.Base != nil {
		b = append(b, p.Base[:]...)
		b = append(b, p.Delta...)
	}
	return b
}

type NamespacePrefix byte

var ErrNamespaceNotFound = errors.New("namespace not found")
var ErrPageNotFound = errors.New("page not found")
var ErrConflict = errors.New("conflict")

var globalEncoder = func() *zstd.Encoder {
	w, err := zstd.NewWriter(nil)
	if err != nil {
		panic(err)
	}
	return w
}()

var globalDecoder = func() *zstd.Decoder {
	r, err := zstd.NewReader(nil)
	if err != nil {
		panic(err)
	}
	return r
}()

type Storage struct {
	Client  *clientv3.Client
	Logger  zerolog.Logger
	KV      clientv3.KV
	Content ContentStorage
}

func New(client *clientv3.Client, logger zerolog.Logger) *Storage {
	return &Storage{
		Client:  client,
		Logger:  logger,
		KV:      clientv3.NewKV(client),
		Content: nil, // TODO
	}

}

func namespaceKey(namespace string) string {
	return "n/" + namespace
}

func lwvKey(prefix NamespacePrefix) string {
	return string(prefix) + "/t"
}

func pageKey(prefix NamespacePrefix, index uint32) string {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, index)
	return string(prefix) + "/p/" + string(b)
}

func contentKey(prefix NamespacePrefix, hash models.Hash) string {
	return string(prefix) + "/c/" + string(hash[:])
}

func (s *Storage) GetNamespacePrefix(ctx context.Context, namespace string) (NamespacePrefix, error) {
	get, err := s.KV.Get(ctx, namespaceKey(namespace))
	if err != nil {
		return 0, err
	}
	if get.Count == 0 {
		return 0, ErrNamespaceNotFound
	}

	if len(get.Kvs[0].Value) != 1 {
		return 0, errors.New("invalid namespace prefix")
	}
	return NamespacePrefix(get.Kvs[0].Value[0]), nil
}

func (s *Storage) GetOrCreateNamespace(ctx context.Context, namespace string) (NamespacePrefix, error) {
	session, err := concurrency.NewSession(s.Client, concurrency.WithContext(ctx))
	if err != nil {
		return 0, err
	}
	defer func(session *concurrency.Session) {
		err := session.Close()
		if err != nil {
			s.Logger.Error().Err(err).Msg("failed to close session")
		}
	}(session)

	m := concurrency.NewMutex(session, "ns_lock/")
	err = m.Lock(ctx)
	if err != nil {
		return 0, err
	}
	defer func(m *concurrency.Mutex) {
		err := m.Unlock(ctx)
		if err != nil {
			s.Logger.Error().Err(err).Msg("failed to unlock mutex")
		}
	}(m)

	key := namespaceKey(namespace)

	rangeResp, err := s.KV.Get(
		ctx,
		"n/",
		clientv3.WithRange(clientv3.GetPrefixRangeEnd("n/")),
		clientv3.WithSort(clientv3.SortByValue, clientv3.SortAscend),
	)

	if err != nil {
		return 0, err
	}

	nextByte := byte(0)
	for _, kv := range rangeResp.Kvs {
		if len(kv.Value) != 1 {
			return 0, errors.New("found invalid namespace prefix")
		}
		if string(kv.Key) == key {
			return NamespacePrefix(kv.Value[0]), nil
		}
		if kv.Value[0] == nextByte {
			nextByte++ // TODO: Test this finds the first gap
		}
	}

	// checking here afterward to allow for namespace already existing
	if len(rangeResp.Kvs) > 255 {
		return 0, errors.New("too many namespaces")
	}
	_, err = s.KV.Put(ctx, key, string([]byte{nextByte}))
	if err != nil {
		return 0, err
	}

	return NamespacePrefix(nextByte), nil
}

func (s *Storage) GetVersion(ctx context.Context, prefix NamespacePrefix) (models.Version, error) {
	get, err := s.KV.Get(ctx, lwvKey(prefix))
	if err != nil {
		return 0, err
	}
	return models.Version(get.Header.Revision), nil
}

func (s *Storage) Read(ctx context.Context, prefix NamespacePrefix, request models.ReadRequest) (*models.VersionedPage, error) {
	v, err := models.NewVersionFromString(request.Version)
	if err != nil {
		return nil, err
	}
	get, err := s.KV.Get(ctx, pageKey(prefix, request.PageIndex), clientv3.WithRev(int64(v)), clientv3.WithSerializable())
	if err != nil {
		return nil, err
	}
	if get.Count == 0 {
		return nil, ErrPageNotFound
	}
	ptr, err := readPagePointer(get.Kvs[0].Value)
	if err != nil {
		return nil, err
	}

	var p models.Page
	if ptr.Base != nil {
		p, err = s.Content.GetWithDelta(ctx, prefix, ptr.Hash, *ptr.Base, ptr.Delta)
		if err != nil {
			return nil, err
		}
	} else {
		p, err = s.Content.Get(ctx, prefix, ptr.Hash)
		if err != nil {
			return nil, err
		}
	}
	return &models.VersionedPage{
		Page:    p,
		Version: models.Version(get.Header.Revision),
	}, nil

	//cr, err := s.Content.GetCached(prefix, ptr.Hash)
	//if err != nil {
	//	return nil, err
	//}
	//if cr.Found {
	//	return &compressedPage{
	//		version: models.Version(get.Header.Revision),
	//		r:       cr.Reader,
	//	}, nil
	//}
	//
	//if ptr.Base != nil {
	//	r, err := s.Content.GetCached(prefix, *ptr.Base)
	//	if err != nil {
	//		return nil, err
	//	}
	//	if r.Found {
	//		defer func(Reader io.ReadCloser) {
	//			err := Reader.Close()
	//			if err != nil {
	//				s.Logger.Error().Err(err).Msg("failed to close reader")
	//			}
	//		}(r.Reader)
	//
	//		buf, err := io.ReadAll(r.Reader)
	//		if err != nil {
	//			return nil, err
	//		}
	//		xor, err := globalDecoder.DecodeAll(ptr.Delta, nil)
	//		if err != nil {
	//			return nil, err
	//		}
	//		if len(xor) != len(buf) {
	//			return nil, errors.New("invalid delta") // TODO: Can probably skip
	//		}
	//		dst := make([]byte, len(buf))
	//		subtle.XORBytes(dst, buf, xor)
	//		return &uncompressedPage{
	//			version: models.Version(get.Header.Revision),
	//			r:       io.NopCloser(bytes.NewReader(dst)),
	//		}, nil
	//
	//	}
	//}
	//r, err := s.Content.Get(ctx, prefix, ptr.Hash)
	//if err != nil {
	//	return nil, err
	//}
	//
	//return &compressedPage{
	//	version: models.Version(get.Header.Revision),
	//	r:       r,
	//}, nil
}

func readPagePointer(value []byte) (*PagePointer, error) {
	if len(value) < 32 {
		return nil, errors.New("invalid page pointer")
	}
	var base *models.Hash
	var delta []byte
	if len(value) > 64 {
		base = (*models.Hash)(value[32:64])
		delta = value[64:]
	}
	// example (*[1]byte)(s[1:])
	return &PagePointer{
		Hash:  *(*models.Hash)(value[:32]),
		Base:  base,
		Delta: delta,
	}, nil
}

func (s *Storage) Write(ctx context.Context, prefix NamespacePrefix, data []byte) (models.Hash, error) {
	return s.Content.Put(ctx, prefix, data)
}

type CommitRequest struct {
	PageIndex uint32
	Hash      models.Hash
	Data      []byte
}

func (s *Storage) Commit(ctx context.Context, prefix NamespacePrefix, version models.Version, requests []CommitRequest, readSet []uint32) (models.Version, error) {
	puts := make([]clientv3.Op, len(requests))
	hashes := make([]models.Hash, len(requests))
	cmpSet := map[uint32]struct{}{}
	for _, p := range readSet {
		cmpSet[p] = struct{}{}
	}
	for i, r := range requests { // Potentially need to handle duplicates here, should test
		if len(r.Data) > 0 {

			hash, err := s.Write(ctx, prefix, r.Data)
			if err != nil {
				return 0, err
			}
			if hash != r.Hash {
				return 0, errors.New("hash mismatch")
			}
		}
		cmpSet[r.PageIndex] = struct{}{}
		// TODO: Delta
		p := PagePointer{Hash: r.Hash}
		puts[i] = clientv3.OpPut(pageKey(prefix, r.PageIndex), string(p.MarshalBinary()))
		hashes[i] = r.Hash
	}
	ok, c, err := s.Content.Ensure(ctx, prefix, hashes)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, errors.New("failed to ensure content")
	}

	cmps := make([]clientv3.Cmp, 0, len(cmpSet)+len(c))
	for p, _ := range cmpSet {
		cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(pageKey(prefix, p)), "<", int64(version)+1))
	}
	for _, cmp := range c {
		cmps = append(cmps, cmp)
	}

	r, err := s.KV.Txn(ctx).If(cmps...).Then(puts...).Commit()
	if err != nil {
		return 0, err
	}
	if !r.Succeeded {
		return 0, ErrConflict
	}
	return models.Version(r.Header.Revision), nil
}
