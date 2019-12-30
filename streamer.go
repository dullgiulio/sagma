package sagma

import (
	"compress/gzip"
	"compress/zlib"
	"fmt"
	"io"
)

type closeboth struct {
	under  io.WriteCloser
	target io.WriteCloser
}

func (c *closeboth) Write(buf []byte) (int, error) {
	return c.target.Write(buf)
}

func (c *closeboth) Close() error {
	err := c.target.Close()
	err1 := c.under.Close()
	if err1 != nil {
		err = err1
	}
	return err
}

type StoreStreamer interface {
	Filename(filename string) string                 // modify filename
	Writer(w io.WriteCloser) (io.WriteCloser, error) // modify writer
	Reader(r io.ReadCloser) (io.ReadCloser, error)   // modify reader
}

type NopStreamer struct{}

func (NopStreamer) Filename(filename string) string {
	return filename
}

func (NopStreamer) Writer(w io.WriteCloser) (io.WriteCloser, error) {
	return w, nil
}

func (NopStreamer) Reader(r io.ReadCloser) (io.ReadCloser, error) {
	return r, nil
}

type ZlibStreamer struct{}

func (ZlibStreamer) Filename(filename string) string {
	return filename + ".z"
}

func (ZlibStreamer) Writer(w io.WriteCloser) (io.WriteCloser, error) {
	return &closeboth{under: w, target: zlib.NewWriter(w)}, nil
}

func (ZlibStreamer) Reader(r io.ReadCloser) (io.ReadCloser, error) {
	zr, err := zlib.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("cannot start zlib decompressor on contents file: %v", err)
	}
	return zr, nil
}

type GzipStreamer struct{}

func (GzipStreamer) Filename(filename string) string {
	return filename + ".gz"
}

func (GzipStreamer) Writer(w io.WriteCloser) (io.WriteCloser, error) {
	return &closeboth{under: w, target: gzip.NewWriter(w)}, nil
}

func (GzipStreamer) Reader(r io.ReadCloser) (io.ReadCloser, error) {
	zr, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("cannot start zlib decompressor on contents file: %v", err)
	}
	return zr, nil
}
