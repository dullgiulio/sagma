package sagma

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type blobBasename string

type blobFolder string

func (b blobFolder) basename(id MsgID, st State) blobBasename {
	return blobBasename(blobStoreID(id, st))
}

func (b blobFolder) file(streamer StoreStreamer, name blobBasename) string {
	return filepath.Join(string(b), string(name[0:2]), streamer.Filename(string(name)))
}

type FileBlobStore struct {
	streamer StoreStreamer
	folder   blobFolder
}

func NewFileBlobStore(folder string, streamer StoreStreamer) *FileBlobStore {
	if streamer == nil {
		streamer = NopStreamer{}
	}
	return &FileBlobStore{
		folder:   blobFolder(folder),
		streamer: streamer,
	}
}

func (s *FileBlobStore) Put(id MsgID, state State, body io.Reader) (BlobID, error) {
	var blobID BlobID
	basename := s.folder.basename(id, state)
	filename := s.folder.file(s.streamer, basename)
	if err := os.MkdirAll(filepath.Dir(string(filename)), 0744); err != nil {
		return blobID, fmt.Errorf("cannot make message folder in blob store: %w", err)
	}
	if err := writeFile(s.streamer, filename, body, 0644); err != nil {
		return blobID, fmt.Errorf("cannot write file to blob store: %w", err)
	}
	blobID = BlobID(basename)
	return blobID, nil
}

func (s *FileBlobStore) Get(id MsgID, state State) (io.ReadCloser, error) {
	basename := s.folder.basename(id, state)
	filename := s.folder.file(s.streamer, basename)
	fh, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("cannot open blob store file: %w", err)
	}
	r, err := s.streamer.Reader(fh)
	if err != nil {
		return nil, fmt.Errorf("cannot wrap reader with streamer: %w", err)
	}
	return r, nil
}

func (s *FileBlobStore) Delete(blobID BlobID) error {
	filename := s.folder.file(s.streamer, blobBasename(blobID))
	if err := os.Remove(filename); err != nil {
		return fmt.Errorf("cannot remove blob: %w", err)
	}
	return nil
}

func writeFile(streamer StoreStreamer, filename string, r io.Reader, perm os.FileMode) error {
	tmpname := filename + ".tmp"
	f, err := os.OpenFile(tmpname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	w, err := streamer.Writer(f)
	if err != nil {
		return err
	}
	_, err = io.Copy(w, r)
	if err1 := w.Close(); err == nil {
		err = err1
	}
	if err == nil {
		if err1 := os.Rename(tmpname, filename); err1 != nil {
			os.Remove(tmpname) // cleanup is best effort only
			err = err1
		}
	} else {
		os.Remove(tmpname)
	}
	return err
}
