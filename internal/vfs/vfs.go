package vfs

import (
	"errors"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/psanford/sqlite3vfs"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	bolt "go.etcd.io/bbolt"
	"io"
	"os"
	"path/filepath"
	pageSchema "s3qlite/internal/schema/page"
	"strings"
	"sync"
	"sync/atomic"
)

type dbRef struct {
	db    *bolt.DB
	count uint
}

type globalState struct {
	dbs   map[string]*dbRef
	mutex sync.Mutex
}

var global = globalState{
	dbs:   make(map[string]*dbRef),
	mutex: sync.Mutex{},
}

type VFS struct {
	tmp    *TmpVFS
	state  *globalState
	logger zerolog.Logger
}

func NewVFS() *VFS {
	return &VFS{
		tmp:    newTempVFS(),
		state:  &global,
		logger: log.Output(zerolog.ConsoleWriter{Out: os.Stderr}),
	}
}

func (v *VFS) Open(name string, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	if flags&sqlite3vfs.OpenWAL != 0 {
		return nil, 0, errors.New("WAL mode not supported")
	}

	if flags&sqlite3vfs.OpenMainDB == 0 {
		return v.tmp.Open(name, flags)
	}

	dbName, _ := strings.CutPrefix(name, "/")
	v.state.mutex.Lock()
	defer v.state.mutex.Unlock()
	db, ok := v.state.dbs[dbName]
	if !ok {
		db = &dbRef{
			count: 0,
		}
		options := *bolt.DefaultOptions
		options.PageSize = 1 << 16 // 64k - Larger pages to avoid overflow

		// TODO: Deal with read only opening and other flags
		var err error
		db.db, err = bolt.Open(filepath.Join(v.tmp.tmpdir, dbName), 0600, &options)
		if err != nil {
			return nil, 0, err
		}
		err = db.db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists(pagesKey)
			if err != nil {
				return err
			}
			fp := b.Get(offsetKey(0))
			if fp == nil {
				builder := flatbuffers.NewBuilder(0)
				realData := builder.CreateByteVector(firstPageTemplate)
				pageSchema.RealStart(builder)
				pageSchema.RealAddData(builder, realData)
				realPtr := pageSchema.RealEnd(builder)
				pageSchema.PageStart(builder)
				pageSchema.PageAddRevision(builder, 0)
				pageSchema.PageAddDataType(builder, pageSchema.DataReal)
				pageSchema.PageAddData(builder, realPtr)
				root := pageSchema.PageEnd(builder)
				builder.Finish(root)
				buf := builder.FinishedBytes()
				err = b.Put(offsetKey(0), buf)
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			_ = db.db.Close()
			return nil, 0, err
		}
		v.state.dbs[dbName] = db
	}
	db.count++

	return NewFile(v, dbName), flags, nil
}

func (v *VFS) Delete(name string, dirSync bool) error {
	return v.tmp.Delete(name, dirSync)
}

func (v *VFS) Access(name string, flags sqlite3vfs.AccessFlag) (bool, error) {
	return v.tmp.Access(name, flags)
}

func (v *VFS) FullPathname(name string) string {
	return v.tmp.FullPathname(name)
}

type TmpVFS struct {
	tmpdir string
}

func newTempVFS() *TmpVFS {

	dir, err := os.MkdirTemp( /*os.Getenv("TMPDIR")*/ "/Users/bwarminski/src/bolt-sandbox", "sqlite3vfs_test_tmpvfs")
	if err != nil {
		panic(err)
	}

	return &TmpVFS{
		tmpdir: dir,
	}
}

func (vfs *TmpVFS) Open(name string, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	var (
		f   *os.File
		err error
	)

	if name == "" {
		f, err = os.CreateTemp(vfs.tmpdir, "")
		if err != nil {
			return nil, 0, sqlite3vfs.CantOpenError
		}
	} else {
		fname := filepath.Join(vfs.tmpdir, name)
		if !strings.HasPrefix(fname, vfs.tmpdir) {
			return nil, 0, sqlite3vfs.PermError
		}
		var fileFlags int
		if flags&sqlite3vfs.OpenExclusive != 0 {
			fileFlags |= os.O_EXCL
		}
		if flags&sqlite3vfs.OpenCreate != 0 {
			fileFlags |= os.O_CREATE
		}
		if flags&sqlite3vfs.OpenReadOnly != 0 {
			fileFlags |= os.O_RDONLY
		}
		if flags&sqlite3vfs.OpenReadWrite != 0 {
			fileFlags |= os.O_RDWR
		}
		f, err = os.OpenFile(fname, fileFlags, 0600)
		if err != nil {
			return nil, 0, sqlite3vfs.CantOpenError
		}
	}

	tf := &TmpFile{f: f}
	return tf, flags, nil
}

func (vfs *TmpVFS) Delete(name string, dirSync bool) error {
	fname := filepath.Join(vfs.tmpdir, name)
	if !strings.HasPrefix(fname, vfs.tmpdir) {
		return errors.New("illegal path")
	}
	return os.Remove(fname)
}

func (vfs *TmpVFS) Access(name string, flag sqlite3vfs.AccessFlag) (bool, error) {
	fname := filepath.Join(vfs.tmpdir, name)
	if !strings.HasPrefix(fname, vfs.tmpdir) {
		return false, errors.New("illegal path")
	}

	exists := true
	_, err := os.Stat(fname)
	if err != nil && os.IsNotExist(err) {
		exists = false
	} else if err != nil {
		return false, err
	}

	if flag == sqlite3vfs.AccessExists {
		return exists, nil
	}

	return true, nil
}

func (vfs *TmpVFS) FullPathname(name string) string {
	fname := filepath.Join(vfs.tmpdir, name)
	if !strings.HasPrefix(fname, vfs.tmpdir) {
		return ""
	}

	return strings.TrimPrefix(fname, vfs.tmpdir)
}

type TmpFile struct {
	lockCount int64
	f         *os.File
}

func (tf *TmpFile) Close() error {
	return tf.f.Close()
}

func (tf *TmpFile) ReadAt(p []byte, off int64) (n int, err error) {
	return tf.f.ReadAt(p, off)
}

func (tf *TmpFile) WriteAt(b []byte, off int64) (n int, err error) {
	return tf.f.WriteAt(b, off)
}

func (tf *TmpFile) Truncate(size int64) error {
	return tf.f.Truncate(size)
}

func (tf *TmpFile) Sync(flag sqlite3vfs.SyncType) error {
	return tf.f.Sync()
}

func (tf *TmpFile) FileSize() (int64, error) {
	cur, _ := tf.f.Seek(0, io.SeekCurrent)
	end, err := tf.f.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	_, err = tf.f.Seek(cur, io.SeekStart)
	if err != nil {
		return 0, err
	}
	return end, nil
}

func (tf *TmpFile) Lock(elock sqlite3vfs.LockType) error {
	if elock == sqlite3vfs.LockNone {
		return nil
	}
	atomic.AddInt64(&tf.lockCount, 1)
	return nil
}

func (tf *TmpFile) Unlock(elock sqlite3vfs.LockType) error {
	if elock == sqlite3vfs.LockNone {
		return nil
	}
	atomic.AddInt64(&tf.lockCount, -1)
	return nil
}

func (tf *TmpFile) CheckReservedLock() (bool, error) {
	count := atomic.LoadInt64(&tf.lockCount)
	return count > 0, nil
}

func (tf *TmpFile) SectorSize() int64 {
	return 0
}

func (tf *TmpFile) DeviceCharacteristics() sqlite3vfs.DeviceCharacteristic {
	return 0
}

func (tf *TmpFile) ConfirmCommit() error {
	return nil
}
