package main

import . "github.com/ctripcorp/nephele/storage"

type storage struct {
	dir string
}

func (s *storage) File(key string) File {
	return &file{
		dir: s.dir,
		key: key,
	}
}

func (s *storage) Iterator(prefix string, lastKey string) Iterator {
	return &iterator{
		dir:     s.dir,
		lastKey: lastKey,
	}
}

func (s *storage) StoreFile(key string, blob []byte, kvs ...KV) (string, error) {
	f := s.File(key)
	_, k, err := f.Append(blob, 0, kvs...)
	return k, err
}
