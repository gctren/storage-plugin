package main

import (
	"io/ioutil"
	"os"
	"strings"

	. "github.com/ctripcorp/nephele/storage"
)

type file struct {
	dir  string
	key  string
	blob []byte
	err  error
}

func (f *file) Key() string {
	return join(f.dir, f.key)
}

func join(dir, key string) string {
	str := "/"
	if !strings.HasSuffix(dir, str) {
		dir = dir + str
	}
	if strings.HasPrefix(key, str) {
		key = strings.TrimPrefix(key, str)
	}
	return dir + key
}

func (f *file) Exist() (bool, string, error) {
	_, err := os.Stat(f.Key())
	return err == nil || os.IsExist(err), "", err
}

func (f *file) Meta() (Fetcher, error) {
	fileInfo, err := os.Stat(f.Key())
	if err != nil {
		return nil, err
	}
	return &fetcher{fileInfo: fileInfo}, nil
}

func (f *file) Append(blob []byte, index int64, kvs ...KV) (int64, string, error) {
	fd, err := os.OpenFile(f.Key(), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return 0, "", err
	}
	defer fd.Close()
	off, err := fd.WriteAt(blob, index)
	return int64(off), "", err
}

func (f *file) Delete() (string, error) {
	return "", os.Remove(f.Key())
}

func (f *file) Bytes() ([]byte, string, error) {
	bts, err := ioutil.ReadFile(f.Key())
	return bts, "", err
}

func (f *file) SetMeta(kvs ...KV) error {
	return nil
}
