package mmap

import (
	"fmt"
	"os"
)

// Borrowed and modified from https://github.com/prometheus/prometheus/blob/main/tsdb/fileutil/mmap.go
// to support writes. License: Apache License 2.0

type MmapFile struct {
	f *os.File
	b []byte
}

func OpenMmapFileWithSize(f *os.File, size int) (mf *MmapFile, retErr error) {
	if size <= 0 {
		info, err := f.Stat()
		if err != nil {
			return nil, fmt.Errorf("stat: %w", err)
		}
		size = int(info.Size())
	}

	b, err := mmap(f, size)
	if err != nil {
		return nil, fmt.Errorf("mmap, size %d: %w", size, err)
	}

	return &MmapFile{f: f, b: b}, nil
}

func (f *MmapFile) Close() error {
	err0 := munmap(f.b)
	err1 := f.f.Close()

	if err0 != nil {
		return err0
	}
	return err1
}

func (f *MmapFile) File() *os.File {
	return f.f
}

func (f *MmapFile) Bytes() []byte {
	return f.b
}
