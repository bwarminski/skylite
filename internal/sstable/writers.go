package sstable

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/thomasjungblut/go-sstables/v2/recordio"
	"github.com/thomasjungblut/go-sstables/v2/recordio/proto"
	"github.com/thomasjungblut/go-sstables/v2/skiplist"
	"github.com/thomasjungblut/go-sstables/v2/sstables"
	"io"
	"os"
	"strings"
)

var ErrNotImplemented = errors.New("not implemented")

func NewS3SSTableWriter(s3Client *s3.Client, bucket string, basePath string) (sstables.SSTableStreamWriterI, error) {
	factory := &StreamingS3Factory{
		S3:     s3Client,
		Bucket: bucket,
	}
	indexFilePath := strings.Join([]string{basePath, sstables.IndexFileName}, "/")
	dataFilePath := strings.Join([]string{basePath, sstables.DataFileName}, "/")
	metaFilePath := strings.Join([]string{basePath, sstables.MetaFileName}, "/")

	var dataWriter recordio.WriterI
	var metaFile *os.File
	var w *sstables.SSTableStreamWriter
	indexWriter, err := proto.NewWriter(proto.Path(indexFilePath), proto.Factory(factory))

	if err != nil {
		goto cleanup
	}

	dataWriter, err = recordio.NewFileWriter(
		recordio.Path(dataFilePath),
		recordio.CompressionType(recordio.CompressionTypeSnappy),
		recordio.Factory(factory),
	)

	if err != nil {
		goto cleanup
	}

	metaFile, err = os.CreateTemp("", sstables.MetaFileName)
	if err != nil {
		goto cleanup
	}

	w, err = sstables.NewSSTableStreamWriter(
		sstables.WriteBufferSizeBytes(4096),
		sstables.DataCompressionType(recordio.CompressionTypeSnappy),
		sstables.WithKeyComparator(skiplist.BytesComparator{}),
		sstables.WithIndexWriter(indexWriter),
		sstables.WithDataWriter(dataWriter),
		sstables.WithMetaDataWriter(&S3Uploader{
			S3:     s3Client,
			Bucket: bucket,
			Path:   metaFilePath,
			File:   metaFile,
		},
		),
	)

	if err != nil {
		return nil, err
	}
	return w, nil
cleanup:
	if indexWriter != nil {
		_ = indexWriter.Close()
	}
	if dataWriter != nil {
		_ = dataWriter.Close()
	}
	if metaFile != nil {
		_ = metaFile.Close()
		_ = os.Remove(metaFile.Name())
	}

	return nil, err
}

type StreamingS3Factory struct {
	S3     *s3.Client
	Bucket string
}

func (s *StreamingS3Factory) CreateNewReader(filePath string, bufSize int) (*os.File, recordio.ByteReaderResetCount, error) {
	//TODO implement me
	panic("implement me")
}

func (s *StreamingS3Factory) CreateNewWriter(filePath string, bufSize int) (recordio.NamedSyncer, recordio.WriteCloserFlusher, error) {
	reader, writer := io.Pipe()
	done := make(chan error)

	go func() {
		defer close(done)
		_, err := s.S3.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(s.Bucket),
			Key:    aws.String(filePath),
			Body:   reader,
		})
		done <- err
	}()

	s3Writer := &S3Writer{
		name:   filePath,
		writer: writer,
		done:   done,
	}
	return s3Writer, s3Writer, nil
}

type S3Writer struct {
	name   string
	writer *io.PipeWriter
	done   chan error
	size   int
}

func (s *S3Writer) Write(p []byte) (n int, err error) {
	n, err = s.writer.Write(p)
	s.size = s.size + n
	return
}

func (s *S3Writer) Flush() error {
	return nil
}

func (s *S3Writer) Size() int {
	return s.size
}

func (s *S3Writer) Sync() error {
	return ErrNotImplemented
}

func (s *S3Writer) Name() string {
	return s.name
}

func (s *S3Writer) Close() error {
	_ = s.writer.CloseWithError(io.EOF)
	err := <-s.done
	if err != io.EOF {
		return err
	}
	return nil
}

type UploadingS3Factory struct {
	S3     *s3.Client
	Bucket string
}

func (u *UploadingS3Factory) CreateNewReader(filePath string, bufSize int) (*os.File, recordio.ByteReaderResetCount, error) {
	//TODO implement me
	panic("implement me")
}

func (u *UploadingS3Factory) CreateNewWriter(filePath string, bufSize int) (recordio.NamedSyncer, recordio.WriteCloserFlusher, error) {
	f, err := os.CreateTemp("", "sstable")
	if err != nil {
		return nil, nil, err
	}
	return f, &S3Uploader{
		S3:     u.S3,
		Bucket: u.Bucket,
		Path:   filePath,
		File:   f,
	}, nil
}

type S3Uploader struct {
	S3      *s3.Client
	Bucket  string
	Path    string
	File    *os.File
	written int
}

func (s *S3Uploader) Write(p []byte) (n int, err error) {
	n, err = s.File.Write(p)
	s.written = s.written + n
	return
}

func (s *S3Uploader) Close() error {
	_, err := s.File.Seek(0, 0)
	if err != nil {
		return err
	}

	defer func(File *os.File) {
		_ = File.Close()
		_ = os.Remove(File.Name())
	}(s.File)

	_, err = s.S3.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(s.Path),
		Body:   s.File,
	})
	return err
}

func (s *S3Uploader) Flush() error {
	return nil
}

func (s *S3Uploader) Size() int {
	return s.written
}
