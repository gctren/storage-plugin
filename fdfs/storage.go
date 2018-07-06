package main

import (
	"time"

	. "github.com/ctripcorp/nephele/storage"
)

type storage struct {
	host           string
	port           int
	socketPoolSize int
	socketInitSize int
	connectTimeout time.Duration
	socketIdleTime time.Duration
	ioTimeout      time.Duration
}

func (s *storage) File(key string) File {
	return &file{
		host:           s.host,
		port:           s.port,
		socketPoolSize: s.socketPoolSize,
		socketInitSize: s.socketInitSize,
		connectTimeout: s.connectTimeout,
		socketIdleTime: s.socketIdleTime,
		ioTimeout:      s.ioTimeout,
		key:            key,
	}
}

func (s *storage) Iterator(prefix string, lastKey string) Iterator {
	return nil
}

func (s *storage) StoreFile(key string, blob []byte, kvs ...KV) (string, error) {
	f := s.File(key)
	_, k, err := f.Append(blob, 0, kvs...)
	return k, err
}
