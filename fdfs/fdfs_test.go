package main

import (
	"strings"
	"testing"
)

func Test_Fast(t *testing.T) {
	fdfsClient, err := newfdfsClient1("host", 22122)
	if err != nil {
		t.Error(err)
		return
	}
	content := []byte("testest")

	path, err := fdfsClient.UploadByBuffer("g1", content, "txt")
	if err != nil {
		t.Error(err)
		return
	}
	t.Error("upload success")

	bts, err := fdfsClient.DownloadToBuffer(path)
	if err != nil {
		t.Error(err)
		return
	}
	if string(content) != string(bts) {
		t.Error("download file err. bts:", string(bts))
		return
	}
	t.Error("download success")

	if err = fdfsClient.DeleteFile(path); err != nil {
		t.Error(err)
		return
	}
	t.Error("delete success")

	_, err = fdfsClient.DownloadToBuffer(path)
	if err != nil {
		if !strings.Contains(err.Error(), "receive status: 2 != 0") {
			t.Error(err)
			return
		}
		return
	}
}
