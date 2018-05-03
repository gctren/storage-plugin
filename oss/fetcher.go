package main

import (
	"net/http"

	"github.com/ctrip-nephele/aliyun-oss-go-sdk/oss"
)

type fetcher struct {
	header http.Header
}

func (m *fetcher) Fetch(key string) string {
	return m.header.Get(oss.HTTPHeaderOssMetaPrefix + key)
}
