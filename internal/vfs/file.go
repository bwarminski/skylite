package vfs

import (
	_ "embed"
	"encoding/binary"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/huandu/skiplist"
	"github.com/psanford/sqlite3vfs"
	bolt "go.etcd.io/bbolt"
	"runtime/debug"
	pageSchema "s3qlite/internal/schema/page"
)

const SectorSize = 4096

//go:embed template_4k.db
var firstPageTemplate []byte
var pagesKey []byte = []byte("pages")

type PageRevision struct {
	Offset int64 // TODO: Update library to make this uint64
	Rev    int64
}

type File struct {
	vfs             *VFS
	db              *bolt.DB
	name            string
	sectorSize      uint64
	lock            sqlite3vfs.LockType
	txn             *bolt.Tx
	revisions       *skiplist.SkipList
	versionCounter  uint32
	firstPage       []byte
	commitConfirmed bool
}

func NewFile(vfs *VFS, name string) *File {
	firstPage := make([]byte, len(firstPageTemplate))
	copy(firstPage, firstPageTemplate)
	return &File{
		vfs:        vfs,
		db:         vfs.state.dbs[name].db,
		name:       name,
		sectorSize: SectorSize,
		lock:       sqlite3vfs.LockNone,
		txn:        nil,
		revisions: skiplist.New(skiplist.GreaterThanFunc(func(k1, k2 interface{}) int {
			cmp := k1.(PageRevision).Offset - k2.(PageRevision).Offset
			if cmp != 0 {
				return int(cmp)
			}

			return int(k1.(PageRevision).Rev - k2.(PageRevision).Rev)
		})),
		versionCounter:  0,
		firstPage:       firstPage,
		commitConfirmed: false,
	}
}

func (f *File) Close() error {
	// TODO: This implementation is very tightly coupled with VFS, it should be refactored
	if f.txn != nil {
		err := f.txn.Rollback()
		if err != nil && err != bolt.ErrTxClosed {
			f.vfs.logger.Error().Err(err).Msg("ignoring error rolling back transaction")
		}
	}
	f.vfs.state.mutex.Lock()
	defer f.vfs.state.mutex.Unlock()
	ref, ok := f.vfs.state.dbs[f.name]
	if !ok || ref.db != f.db {
		f.vfs.logger.Error().Msg("db not found in vfs state")
		return sqlite3vfs.InternalError
	}
	if ref.count == 0 {
		f.vfs.logger.Error().Msg("db count is already 0")
		return sqlite3vfs.InternalError
	}
	ref.count--
	if ref.count == 0 {
		err := ref.db.Close()
		if err != nil {
			f.vfs.logger.Error().Err(err).Msg("error closing db")
			return sqlite3vfs.IOError
		}
	}
	return nil
}

func (f *File) ReadAt(p []byte, off int64) (n int, err error) {
	f.vfs.logger.Debug().Int64("offset", off).Msg("read at")
	if f.txn == nil {
		if off != 0 {
			f.vfs.logger.Error().Msg("unexpected read offset without transaction")
			return 0, sqlite3vfs.IOError
		}
		if len(p) != 100 {
			f.vfs.logger.Warn().Msg("read on first page called with non-standard size")
		}
		return copy(p, f.firstPage), nil
	}

	if off%SectorSize != 0 || len(p)%SectorSize != 0 {
		// Indicates a partial read of the first page
		if off >= SectorSize || len(p) >= SectorSize || off+int64(len(p)) > SectorSize {
			f.vfs.logger.Error().Msg("unexpected read offset or size")
			return 0, sqlite3vfs.IOError
		}

		page, err2 := f.readPage(0)
		if err2 != nil {
			f.vfs.logger.Error().Msg("first page not found")
			return 0, err2
		}
		return copy(p, page[off:]), nil
	}

	// Technically contiguous pages could be read here, mvsqlite doesn't read them all for some reason

	n = 0
	for n < len(p) {
		page, err := f.readPage(off)
		if err != nil {
			f.vfs.logger.Error().Bytes("stacktrace", debug.Stack()).Msg("page not found")
			return 0, err
		}
		copied := copy(p[n:], page)
		n += copied
		off += int64(copied)
	}
	return n, nil
}

type readPageOptions struct {
	dontRecord       bool
	defaultFirstPage bool
}

type readPageOption func(*readPageOptions)

func dontRecord() readPageOption {
	return func(o *readPageOptions) {
		o.dontRecord = true
	}
}

func defaultFirstPage() readPageOption {
	return func(o *readPageOptions) {
		o.defaultFirstPage = true
	}
}

func (f *File) readPage(off int64, opts ...readPageOption) (result []byte, err error) {
	options := readPageOptions{}
	for _, o := range opts {
		o(&options)
	}
	// Catch panics
	defer func() {
		if r := recover(); r != nil {
			f.vfs.logger.Error().Interface("panic", r).Bytes("stack", debug.Stack()).Msg("panic caught")
			err = sqlite3vfs.IOError
		}
	}()
	page, found := f.rawPage(off)
	if !found {
		if off == 0 && options.defaultFirstPage { // TODO: We can get rid of this
			return f.firstPage, nil
		}
		f.vfs.logger.Error().Bytes("stacktrace", debug.Stack()).Msg("page not found")
		return nil, sqlite3vfs.IOError

	}

	unionTable := new(flatbuffers.Table)
	if !page.Data(unionTable) {
		f.vfs.logger.Error().Msg("page data not found")
		return nil, sqlite3vfs.IOError
	}

	if !options.dontRecord {
		f.revisions.Set(PageRevision{Offset: off, Rev: page.Revision()}, struct{}{})
	}

	if page.DataType() != pageSchema.DataReal {
		// TODO
		panic("data is a ref or none and we're not ready for that")
	}
	realData := new(pageSchema.Real)
	realData.Init(unionTable.Bytes, unionTable.Pos)
	bytes := realData.DataBytes()
	if off == 0 {
		return spliceVersion(bytes, f.versionCounter), nil
	}
	return bytes, nil
}

func (f *File) rawPage(off int64) (*pageSchema.Page, bool) {
	buf := f.txn.Bucket(pagesKey).Get(offsetKey(off))
	if buf == nil {
		return nil, false
	}

	page := pageSchema.GetRootAsPage(buf, 0)
	return page, true
}

func spliceVersion(bytes []byte, version uint32) []byte {
	buf := make([]byte, 0, len(bytes))
	buf = append(buf, bytes[0:24]...)
	buf = binary.BigEndian.AppendUint32(buf, version)
	buf = append(buf, bytes[28:92]...)
	buf = binary.BigEndian.AppendUint32(buf, version)
	buf = append(buf, bytes[96:]...)
	return buf
}

func offsetKey(off int64) []byte {
	return binary.BigEndian.AppendUint64(nil, uint64(off))
}

func (f *File) WriteAt(p []byte, off int64) (n int, err error) {
	f.vfs.logger.Debug().Int64("offset", off).Msg("write at")
	if off%SectorSize != 0 {
		f.vfs.logger.Error().Msg("unexpected write offset")
		return 0, sqlite3vfs.IOError
	}

	if len(p) != SectorSize {
		f.vfs.logger.Error().Msg("unexpected write size")
		return 0, sqlite3vfs.IOError
	}

	if f.txn == nil || !f.txn.Writable() {
		f.vfs.logger.Error().Msg("unexpected write without transaction")
		return 0, sqlite3vfs.IOError
	}

	if off == 0 {
		// validate page size didn't change (taken from mvsqlite)
		pageSize := binary.BigEndian.Uint16(p[16:18])
		if pageSize != SectorSize {
			f.vfs.logger.Error().Msg("attempting to change page size")
			return 0, sqlite3vfs.IOError
		}
		if p[18] == 2 || p[19] == 2 {
			f.vfs.logger.Error().Msg("attempting to enable WAL mode")
			return 0, sqlite3vfs.IOError
		}
		p = spliceVersion(p, 0)
	}

	for n < len(p) {
		err = f.writePage(p[n:], off)
		if err != nil {
			f.vfs.logger.Error().Msg("error writing page")
			return n, err
		}
		n += SectorSize
		off += SectorSize
	}
	return n, err
}

func (f *File) writePage(p []byte, off int64) (err error) {
	// Catch panics
	defer func() {
		if r := recover(); r != nil {
			f.vfs.logger.Error().Interface("panic", r).Msg("panic caught")
			err = sqlite3vfs.IOError
		}
	}()
	builder := flatbuffers.NewBuilder(0)
	realData := builder.CreateByteVector(p)
	pageSchema.RealStart(builder)
	pageSchema.RealAddData(builder, realData)
	realPtr := pageSchema.RealEnd(builder)
	pageSchema.PageStart(builder)
	pageSchema.PageAddRevision(builder, int64(f.versionCounter))
	pageSchema.PageAddDataType(builder, pageSchema.DataReal)
	pageSchema.PageAddData(builder, realPtr)
	root := pageSchema.PageEnd(builder)
	builder.Finish(root)
	buf := builder.FinishedBytes()

	err = f.txn.Bucket(pagesKey).Put(offsetKey(off), buf)
	if err != nil {
		f.vfs.logger.Error().Err(err).Msg("error writing page")
		return sqlite3vfs.IOError
	}
	return nil
}

func (f *File) Truncate(size int64) error {
	// TODO: Actually clear unused space
	return nil
}

func (f *File) Sync(flag sqlite3vfs.SyncType) error {
	return nil
}

func (f *File) FileSize() (int64, error) {
	// TODO: Read max offset instead
	if f.txn == nil {
		f.vfs.logger.Warn().Msg("unexpected file size call without transaction")
		return SectorSize, nil
	}
	p, err := f.readPage(0, dontRecord(), defaultFirstPage())
	if err != nil {
		f.vfs.logger.Error().Msg("error reading first page")
		return 0, err
	}
	nPages := binary.BigEndian.Uint32(p[28:32])
	return int64(nPages) * SectorSize, nil
}

func (f *File) Lock(elock sqlite3vfs.LockType) error {
	f.vfs.logger.Debug().Str("lock", elock.String()).Msg("lock")
	if elock == sqlite3vfs.LockNone {
		f.vfs.logger.Error().Msg("unexpected LockNone received")
		return sqlite3vfs.InternalError
	}
	if f.lock == elock {
		return nil
	}
	if f.lock > elock {
		f.vfs.logger.Error().Msg("lock received unexpected lower type")
		return sqlite3vfs.IOError
	}

	if f.txn == nil {
		if elock != sqlite3vfs.LockShared {
			f.vfs.logger.Error().Msg("unexpected lock type received with no transaction")
			return sqlite3vfs.IOError
		}
		var err error
		f.txn, err = f.db.Begin(false)
		if err != nil {
			f.vfs.logger.Error().Err(err).Msg("error starting transaction")
			return sqlite3vfs.IOError
		}
		f.revisions.Init()
	} else if elock == sqlite3vfs.LockReserved {
		// Replace the transaction with a writable transaction
		// Note: We're maintaining a revisions map, so we can check that they haven't changed when switching to a write transaction
		err := f.txn.Rollback()
		if err != nil {
			f.vfs.logger.Error().Err(err).Msg("error rolling back transaction")
			return sqlite3vfs.IOError
		}
		f.txn, err = f.db.Begin(true)
		if err != nil {
			f.vfs.logger.Error().Err(err).Msg("error starting write transaction")
			return sqlite3vfs.IOError
		}
		f.versionCounter += 2 // Busts the SQLite page cache - //TODO test overflow behavior

		// phantom read check
		if f.revisions.Len() > 0 {
			elem := f.revisions.Front()
			for elem != nil {
				rev := elem.Key().(PageRevision)
				page, _ := f.rawPage(rev.Offset)
				if page == nil {
					f.vfs.logger.Error().Msg("error reading page")
					return sqlite3vfs.IOError
				}
				if rev.Rev != page.Revision() {
					f.vfs.logger.Error().Msg("phantom read detected")
					err = f.txn.Rollback()
					if err != nil {
						f.vfs.logger.Error().Err(err).Msg("error rolling back transaction")
					}
					return sqlite3vfs.BusyError
				}
				elem = elem.Next()
			}
		}
	}
	f.lock = elock
	return nil
}

func (f *File) ConfirmCommit() error {
	if f.txn == nil || !f.txn.Writable() {
		f.vfs.logger.Error().Msg("unexpected commit confirmation without transaction")
		return sqlite3vfs.IOError
	}
	if f.lock <= sqlite3vfs.LockReserved {
		f.vfs.logger.Error().Msg("unexpected commit confirmation without reserved lock")
		return sqlite3vfs.IOError
	}
	if f.commitConfirmed {
		f.vfs.logger.Error().Msg("commit already confirmed")
		return sqlite3vfs.IOError
	}
	f.commitConfirmed = true
	return nil

}

func (f *File) Unlock(elock sqlite3vfs.LockType) error {
	if elock == f.lock {
		return nil
	}

	prevLock := f.lock
	f.lock = elock
	if prevLock >= sqlite3vfs.LockReserved && elock < sqlite3vfs.LockReserved {
		if f.txn == nil || !f.txn.Writable() {
			f.vfs.logger.Error().Msg("unexpected unlock without transaction")
			return sqlite3vfs.IOError
		}
		var err error
		if !f.commitConfirmed {
			err = f.txn.Rollback()
			if err != nil {
				f.vfs.logger.Error().Err(err).Msg("error rolling back transaction")
			}
		} else {
			err = f.txn.Commit()
			if err != nil {
				f.vfs.logger.Error().Err(err).Msg("error committing transaction")
				return sqlite3vfs.IOError
			}
		}

		var err2 error
		f.txn, err2 = f.db.Begin(false)
		if err2 != nil {
			f.vfs.logger.Error().Err(err2).Msg("error replacing transaction")
		}
		f.commitConfirmed = false
		if err != nil || err2 != nil {
			return sqlite3vfs.IOError
		}
		return nil
	}

	if elock == sqlite3vfs.LockNone {
		if f.txn != nil {
			err := f.txn.Rollback()
			if err != nil && err != bolt.ErrTxClosed {
				f.vfs.logger.Error().Err(err).Msg("error rolling back transaction")
				return sqlite3vfs.IOError
			}
			f.txn = nil
		}
	}
	return nil
}

func (f *File) CheckReservedLock() (bool, error) {
	return f.lock > sqlite3vfs.LockNone, nil
}

func (f *File) SectorSize() int64 {
	return SectorSize
}

func (f *File) DeviceCharacteristics() sqlite3vfs.DeviceCharacteristic {
	return 0
}
