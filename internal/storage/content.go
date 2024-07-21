package storage

import (
	"bytes"
	"context"
	clientv3 "go.etcd.io/etcd/client/v3"
	"io"
	"lukechampine.com/blake3"
	"s3qlite/internal/models"
)

type CacheResponse struct {
	Reader io.ReadCloser
	Found  bool
}

type ContentStorage interface {
	Get(ctx context.Context, prefix NamespacePrefix, hash models.Hash) (models.Page, error)
	GetWithDelta(ctx context.Context, prefix NamespacePrefix, hash models.Hash, base models.Hash, compressedDelta []byte) (models.Page, error)
	Ensure(ctx context.Context, prefix NamespacePrefix, hash []models.Hash) (bool, []clientv3.Cmp, error)
	Put(ctx context.Context, prefix NamespacePrefix, data []byte) (models.Hash, error)
}

type pageCacheKey struct {
	prefix NamespacePrefix
	hash   models.Hash
}

type cachedPage struct {
	data []byte
	// storage file pointer
	// data, ref counter
	// if ref counter < 0, clean up and delete from cache
}

func (cp cachedPage) Page() models.Page {
	reader := bytes.NewReader(cp.data)
	return &compressedPage{
		r: io.NopCloser(reader),
	}
}

type contentStorage struct {
	Client *clientv3.Client
	KV     clientv3.KV
}

func NewContentStorage(client *clientv3.Client, cacheSize int) ContentStorage {
	cache, err := lru.New[pageCacheKey, cachedPage](cacheSize)
	if err != nil {
		panic(err)
	}
	return &contentStorage{
		Client: client,
		S3:     s3,
		KV:     clientv3.NewKV(client),
		cache:  cache,
	}
}

func (c *contentStorage) Get(ctx context.Context, prefix NamespacePrefix, hash models.Hash) (models.Page, error) {
	if cp, ok := c.cache.Get(pageCacheKey{prefix: prefix, hash: hash}); ok {
		return cp.Page(), nil
	}
	key := contentKey(prefix, hash)
	get, err := c.KV.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if get.Count == 0 {
		return nil, ErrContentNotFound
	}
	cp := cachedPage{data: []byte(get.Kvs[0].Value)}
	c.cache.ContainsOrAdd(pageCacheKey{prefix: prefix, hash: hash}, cp)
	return cp.Page(), nil
}

func (c *contentStorage) GetWithDelta(ctx context.Context, prefix NamespacePrefix, hash models.Hash, base models.Hash, compressedDelta []byte) (models.Page, error) {
	//TODO implement me
	panic("implement me")
}

func (c *contentStorage) Ensure(ctx context.Context, prefix NamespacePrefix, hash []models.Hash) (bool, []clientv3.Cmp, error) {
	//TODO implement me
	panic("implement me")
}

func (c *contentStorage) Put(ctx context.Context, prefix NamespacePrefix, data []byte) (models.Hash, error) {
	hash := models.Hash(blake3.Sum256(data))
	compressed := globalEncoder.EncodeAll(data, nil)
	key := contentKey(prefix, hash)
	_, err := c.KV.Put(ctx, key, string(compressed))
	if err != nil {
		return hash, err
	}
	c.cache.ContainsOrAdd(pageCacheKey{prefix: prefix, hash: hash}, cachedPage{data: compressed})
	return hash, nil
}
