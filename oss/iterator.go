package main

import (
	"time"

	. "github.com/ctripcorp/nephele/storage"

	"github.com/ctrip-nephele/aliyun-oss-go-sdk/oss"
)

type iterator struct {
	bucket          *oss.Bucket
	prefix, lastKey string
	files           chan *file
}

func (iter *iterator) syncing() {
	for {
		r, err := iter.bucket.ListObjects(oss.Marker(iter.lastKey), oss.Prefix(iter.prefix))
		if err != nil {
			iter.files <- &file{
				err: err,
			}
			time.Sleep(time.Second)
			continue
		}
		if len(r.Objects) == 0 {
			iter.files <- nil
			time.Sleep(time.Second)
			continue
		}
		for _, object := range r.Objects {
			iter.files <- &file{
				bucket: iter.bucket,
				key:    object.Key,
			}
			iter.lastKey = object.Key
		}
	}
}

func (iter *iterator) Next() (File, error) {
	f := <-iter.files
	if f.err != nil {
		return nil, f.err
	}
	return f, nil
}

func (iter *iterator) LastKey() string {
	return iter.lastKey
}
