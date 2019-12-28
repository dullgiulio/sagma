package main

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

type nopStreamer struct{}

func (nopStreamer) Filename(filename string) string {
	return filename
}

func (nopStreamer) Writer(w io.WriteCloser) (io.WriteCloser, error) {
	return w, nil
}

func (nopStreamer) Reader(r io.ReadCloser) (io.ReadCloser, error) {
	return r, nil
}

type zlibStreamer struct{}

func (zlibStreamer) Filename(filename string) string {
	return filename + ".z"
}

func (zlibStreamer) Writer(w io.WriteCloser) (io.WriteCloser, error) {
	return &closeboth{under: w, target: zlib.NewWriter(w)}, nil
}

func (zlibStreamer) Reader(r io.ReadCloser) (io.ReadCloser, error) {
	zr, err := zlib.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("cannot start zlib decompressor on contents file: %v", err)
	}
	return zr, nil
}

type gzipStreamer struct{}

func (gzipStreamer) Filename(filename string) string {
	return filename + ".gz"
}

func (gzipStreamer) Writer(w io.WriteCloser) (io.WriteCloser, error) {
	return &closeboth{under: w, target: gzip.NewWriter(w)}, nil
}

func (gzipStreamer) Reader(r io.ReadCloser) (io.ReadCloser, error) {
	zr, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("cannot start zlib decompressor on contents file: %v", err)
	}
	return zr, nil
}
