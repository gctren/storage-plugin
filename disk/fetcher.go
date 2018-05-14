package main

import "os"

type fetcher struct {
	fileInfo os.FileInfo
}

func (m *fetcher) Fetch(key string) string {
	return ""
}
