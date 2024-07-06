package storage

import (
	"bytes"
	"io"
)

type compressedPage struct {
	r io.ReadCloser
}

func (c *compressedPage) DataCompressed() (io.ReadCloser, error) {
	return c.r, nil
}

func (c *compressedPage) DataUncompressed() (io.ReadCloser, error) {
	buf, err := io.ReadAll(c.r)
	_ = c.r.Close()
	if err != nil {
		return nil, err
	}
	dst, err := globalDecoder.DecodeAll(buf, nil)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(dst)), nil
}

type uncompressedPage struct {
	r io.ReadCloser
}

func (u *uncompressedPage) DataUncompressed() (io.ReadCloser, error) {
	return u.r, nil
}

func (u *uncompressedPage) DataCompressed() (io.ReadCloser, error) {
	buf, err := io.ReadAll(u.r)
	_ = u.r.Close()
	if err != nil {
		return nil, err
	}
	dst := globalEncoder.EncodeAll(buf, nil)
	return io.NopCloser(bytes.NewReader(dst)), nil
}
