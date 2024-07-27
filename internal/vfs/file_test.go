package vfs

import (
	"sync"
	"testing"

	"github.com/psanford/sqlite3vfs"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeVFS() *VFS {
	return &VFS{
		tmp: newTempVFS(),
		state: &globalState{
			dbs:   make(map[string]*dbRef),
			mutex: sync.Mutex{},
		},
		logger: log.Logger,
	}

}

// Tests
func TestFile_ReadAt_FirstPage_Success(t *testing.T) {
	vfsInstance := makeVFS()
	file, _, err := vfsInstance.Open("test.db", sqlite3vfs.OpenMainDB|sqlite3vfs.OpenReadWrite)
	require.NoError(t, err)
	defer cleanup(t)(file)

	data := make([]byte, 100)
	n, err := file.ReadAt(data, 0)

	require.NoError(t, err)
	assert.Equal(t, 100, n, "Should read 100 bytes from the first page")
}

func TestFile_ReadAt_PageNotFound_ReturnsError(t *testing.T) {
	vfsInstance := makeVFS()
	file, _, err := vfsInstance.Open("test.db", sqlite3vfs.OpenMainDB|sqlite3vfs.OpenReadWrite)
	require.NoError(t, err)
	defer cleanup(t)(file)

	lockForRead(t, file)

	_, err = file.ReadAt(make([]byte, SectorSize), SectorSize)

	assert.Equal(t, sqlite3vfs.IOError, err, "Should return an IOError when page is not found")

	unlockForRead(t, file)
}

func cleanup(t *testing.T) func(file sqlite3vfs.File) {
	return func(file sqlite3vfs.File) {
		if file.(*File).txn != nil {
			_ = file.(*File).txn.Rollback()
		}
		err := file.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestFile_WriteAt_Success(t *testing.T) {
	vfsInstance := makeVFS()
	file, _, err := vfsInstance.Open("test.db", sqlite3vfs.OpenMainDB|sqlite3vfs.OpenReadWrite)
	require.NoError(t, err)
	defer cleanup(t)(file)

	data := make([]byte, SectorSize)
	copy(data, "Hello, World!")

	lockForRead(t, file)
	lockForWrite(t, file)

	n, err := file.WriteAt(data, SectorSize)

	require.NoError(t, err)
	assert.Equal(t, SectorSize, n, "Should write one sector size worth of data")

	err = file.(*File).ConfirmCommit()
	require.NoError(t, err)

	unlockForWrite(t, file)

	ret := make([]byte, SectorSize)
	n, err = file.ReadAt(ret, SectorSize)
	require.NoError(t, err)
	assert.Equal(t, data, ret, "Should read the same data that was written")

	unlockForRead(t, file)
}

func TestFile_InvalidOffset(t *testing.T) {
	vfsInstance := makeVFS()
	file, _, err := vfsInstance.Open("test.db", sqlite3vfs.OpenMainDB|sqlite3vfs.OpenReadWrite)
	require.NoError(t, err)
	defer cleanup(t)(file)

	lockForRead(t, file)
	data := make([]byte, 100)
	_, err = file.ReadAt(data, -1)
	assert.Equal(t, sqlite3vfs.IOError, err, "Should return an IOError when offset is negative")

	lockForWrite(t, file)
	buf := make([]byte, SectorSize)
	copy(buf, "Hello world")
	_, err = file.WriteAt(buf, -1)
	assert.Equal(t, sqlite3vfs.IOError, err, "Should return an IOError when offset is negative")

	_, err = file.WriteAt(data, SectorSize)
	assert.Equal(t, sqlite3vfs.IOError, err, "Should return an IOError when data doesn't fit page size")

	unlockForWrite(t, file)
	unlockForRead(t, file)
}

func TestFile_ConcurrentAccess(t *testing.T) {
	vfsInstance := makeVFS()
	file, _, err := vfsInstance.Open("test.db", sqlite3vfs.OpenMainDB|sqlite3vfs.OpenReadWrite)
	require.NoError(t, err)
	defer cleanup(t)(file)

	file2, _, err := vfsInstance.Open("test.db", sqlite3vfs.OpenMainDB|sqlite3vfs.OpenReadWrite)
	require.NoError(t, err)
	defer cleanup(t)(file2)

	lockForRead(t, file)
	lockForWrite(t, file)
	data := make([]byte, SectorSize)
	copy(data, "Hello, World!")

	done := make(chan bool)

	go func() {
		lockForRead(t, file2)
		lockForWrite(t, file2)
		ret := make([]byte, SectorSize)
		_, err = file2.ReadAt(ret, SectorSize)
		require.NoError(t, err)
		assert.Equal(t, data, ret, "Should read the same data that was written")
		unlockForWrite(t, file2)
		unlockForRead(t, file2)
		done <- true
	}()

	_, err = file.WriteAt(data, SectorSize)
	require.NoError(t, err)
	err = file.(*File).ConfirmCommit()
	require.NoError(t, err)

	unlockForWrite(t, file)

	unlockForRead(t, file)
	<-done

}

func TestFile_ConcurrentAccess_PhantomRead(t *testing.T) {
	vfsInstance := makeVFS()
	file, _, err := vfsInstance.Open("test.db", sqlite3vfs.OpenMainDB|sqlite3vfs.OpenReadWrite)
	require.NoError(t, err)
	defer cleanup(t)(file)

	file2, _, err := vfsInstance.Open("test.db", sqlite3vfs.OpenMainDB|sqlite3vfs.OpenReadWrite)
	require.NoError(t, err)
	defer cleanup(t)(file2)

	lockForRead(t, file)
	lockForWrite(t, file)
	data := make([]byte, SectorSize)
	copy(data, "Hello, World!")
	_, err = file.WriteAt(data, SectorSize)
	require.NoError(t, err)
	err = file.(*File).ConfirmCommit()
	require.NoError(t, err)
	unlockForWrite(t, file)

	// start phantom transaction
	lockForWrite(t, file)
	done := make(chan bool)

	go func() {
		defer func() { close(done) }()
		lockForRead(t, file2)
		ret := make([]byte, SectorSize)
		_, err = file2.ReadAt(ret, SectorSize)
		require.NoError(t, err)
		assert.Equal(t, data, ret, "Should read the same data that was written")
		done <- true
		err = file2.Lock(sqlite3vfs.LockReserved)
		assert.Equal(t, sqlite3vfs.BusyError, err, "Should return a BusyError due to phantom read detection")

		unlockForRead(t, file2)
		done <- true
	}()

	<-done

	// Introduce a phantom read by modifying the data in the first transaction
	modifiedData := make([]byte, SectorSize)
	copy(modifiedData, "Phantom Read!")
	_, err = file.WriteAt(modifiedData, SectorSize)
	require.NoError(t, err)
	err = file.(*File).ConfirmCommit()
	require.NoError(t, err)

	unlockForWrite(t, file)
	unlockForRead(t, file)
	<-done
}

func TestFile_FileSize(t *testing.T) {
	vfsInstance := makeVFS()
	file, _, err := vfsInstance.Open("test.db", sqlite3vfs.OpenMainDB|sqlite3vfs.OpenReadWrite)
	require.NoError(t, err)
	defer cleanup(t)(file)

	lockForRead(t, file)
	// Test initial file size
	size, err := file.FileSize()
	require.NoError(t, err)
	assert.Equal(t, int64(SectorSize), size, "Initial file size should be one sector size")
}

func lockForRead(t *testing.T, file sqlite3vfs.File) {
	err := file.Lock(sqlite3vfs.LockShared)
	require.NoError(t, err)
}

func lockForWrite(t *testing.T, file sqlite3vfs.File) {

	err := file.Lock(sqlite3vfs.LockReserved)
	require.NoError(t, err)

	err = file.Lock(sqlite3vfs.LockPending)
	require.NoError(t, err)

	err = file.Lock(sqlite3vfs.LockExclusive)
	require.NoError(t, err)
}

func unlockForWrite(t *testing.T, file sqlite3vfs.File) {

	err := file.Unlock(sqlite3vfs.LockPending)
	require.NoError(t, err)

	err = file.Unlock(sqlite3vfs.LockReserved)
	require.NoError(t, err)

	err = file.Unlock(sqlite3vfs.LockShared)
	require.NoError(t, err)
}

func unlockForRead(t *testing.T, file sqlite3vfs.File) {
	err := file.Unlock(sqlite3vfs.LockNone)
	require.NoError(t, err)
}
