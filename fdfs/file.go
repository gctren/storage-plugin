package main

import (
	"errors"
	"fmt"
	"time"

	. "github.com/ctripcorp/nephele/storage"
)

type file struct {
	host           string
	port           int
	socketPoolSize int
	socketInitSize int
	connectTimeout time.Duration
	socketIdleTime time.Duration
	ioTimeout      time.Duration
	key            string
}

func (f *file) Key() string {
	return f.key
}

func (f *file) Exist() (bool, string, error) {
	return false, "", nil
}

const GROUPKEY = "group"
const EXTKEY = "ext"

func (f *file) Append(blob []byte, index int64, kvs ...KV) (int64, string, error) {
	client, e := f.createClient()
	if e != nil {
		return 0, "", e
	}
	if len(f.key) > 0 {
		return 0, "", client.AppendFile(blob, f.key)
	} else {
		var groupName string
		var ext string
		for _, v := range kvs {
			if v[0] == GROUPKEY {
				groupName = v[1]
			} else if v[0] == EXTKEY {
				ext = v[1]
			}

		}
		if len(groupName) < 1 || len(ext) < 1 {
			return 0, "", errors.New("please set group and ext parameters.")
		}
		p, e := client.UploadByBuffer(groupName, blob, ext)
		if len(p) > 0 {
			f.key = p
		}
		return 0, p, e
	}
}

func (f *file) Delete() (string, error) {
	client, e := f.createClient()
	if e != nil {
		return "", e
	}
	return "", client.DeleteFile(f.key)
}

func (f *file) Bytes() ([]byte, string, error) {
	client, e := f.createClient()
	if e != nil {
		return nil, "", e
	}
	bts, e := client.DownloadToBuffer(f.key)
	return bts, "", e
}

func (f *file) Meta() (Fetcher, error) {
	return nil, nil
}

func (f *file) SetMeta(kvs ...KV) error {
	return nil
}

var sm = newsafeMap()

func (f *file) createClient() (client, error) {
	key := fmt.Sprintf("%s-%d", f.host, f.port)
	v, isExists := sm.Get(key)
	if !isExists {
		fdfsClient, err := newfdfsClient(f.host, f.port, &clientConfig{SocketIdleTime: f.socketIdleTime,
			SocketInitSize: f.socketInitSize,
			SocketPoolSize: f.socketPoolSize,
			ConnectTimeout: f.connectTimeout,
			IoTimeout:      f.ioTimeout})
		if err != nil {
			return nil, err
		}
		v, _ = sm.SetIfNotExist(key, fdfsClient)
	}
	return v.(*fdfsClient), nil
}
