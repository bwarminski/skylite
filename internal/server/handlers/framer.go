/*
Copyright 2015 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package handlers

import (
	"encoding/binary"
	"io"
)

type lengthDelimitedFrameWriter struct {
	w io.Writer
	h [4]byte
}

func NewLengthDelimitedFrameWriter(w io.Writer) io.Writer {
	return &lengthDelimitedFrameWriter{w: w}
}

// Write writes a single frame to the nested writer, prepending it with the length
// in bytes of data (as a 4 byte, bigendian uint32).
func (w *lengthDelimitedFrameWriter) Write(data []byte) (int, error) {
	binary.BigEndian.PutUint32(w.h[:], uint32(len(data)))
	n, err := w.w.Write(w.h[:])
	if err != nil {
		return 0, err
	}
	if n != len(w.h) {
		return 0, io.ErrShortWrite
	}
	return w.w.Write(data)
}

type lengthDelimitedFrameReader struct {
	r         io.ReadCloser
	remaining int
}

// NewLengthDelimitedFrameReader returns an io.Reader that will decode length-prefixed
// frames off of a stream.
//
// The protocol is:
//
//	stream: message ...
//	message: prefix body
//	prefix: 4 byte uint32 in BigEndian order, denotes length of body
//	body: bytes (0..prefix)
//
// If the buffer passed to Read is not long enough to contain an entire frame, io.ErrShortRead
// will be returned along with the number of bytes read.
func NewLengthDelimitedFrameReader(r io.ReadCloser) io.ReadCloser {
	return &lengthDelimitedFrameReader{r: r}
}

// Read attempts to read an entire frame into data. If that is not possible, io.ErrShortBuffer
// is returned and subsequent calls will attempt to read the last frame. A frame is complete when
// err is nil.
func (r *lengthDelimitedFrameReader) Read(data []byte) (int, error) {
	if r.remaining <= 0 {
		header := [4]byte{}
		n, err := io.ReadAtLeast(r.r, header[:4], 4)
		if err != nil {
			return 0, err
		}
		if n != 4 {
			return 0, io.ErrUnexpectedEOF
		}
		frameLength := int(binary.BigEndian.Uint32(header[:]))
		r.remaining = frameLength
	}

	expect := r.remaining
	max := expect
	if max > len(data) {
		max = len(data)
	}
	n, err := io.ReadAtLeast(r.r, data[:max], int(max))
	r.remaining -= n
	if err == io.ErrShortBuffer || r.remaining > 0 {
		return n, io.ErrShortBuffer
	}
	if err != nil {
		return n, err
	}
	if n != expect {
		return n, io.ErrUnexpectedEOF
	}

	return n, nil
}

func (r *lengthDelimitedFrameReader) Close() error {
	return r.r.Close()
}
