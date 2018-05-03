package main

import (
	"bytes"
	"github.com/ctrip-nephele/aliyun-oss-go-sdk/oss"
	. "github.com/ctripcorp/nephele/storage"
)

type storage struct {
	bucket *oss.Bucket
}

func (s *storage) File(key string) File {
	return &file{
		bucket: s.bucket,
		key:    key,
	}
}

func (s *storage) Iterator(prefix string, lastKey string) Iterator {
	iter := &iterator{
		bucket:  s.bucket,
		prefix:  prefix,
		lastKey: lastKey,
		files:   make(chan *file, 100),
	}
	go iter.syncing()
	return iter
}

func (s *storage) StoreFile(key string, blob []byte, KVs ...KV) (string, error) {
	options := make([]oss.Option, 0)
	for _, KV := range KVs {
		options = append(options, oss.Meta(KV[0], KV[1]))
	}
	return s.bucket.PutObject(key, bytes.NewReader(blob), options...)
}
