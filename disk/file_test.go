package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func Test_File(t *testing.T) {
	f := &file{dir: getCurrentPath(), key: "1.txt"}
	blob := []byte("testest")
	//1. create file
	_, _, e := f.Append(blob, 0)
	if e != nil {
		t.Error(e)
		return
	}
	//2. append
	_, _, e = f.Append(blob, 0)
	if e != nil {
		t.Error(e)
		return
	}
	//3. get file
	bts, _, e := f.Bytes()
	if e != nil {
		t.Error(e)
		return
	}
	//4. check file
	if string(bts) != string(blob)+string(blob) {
		t.Error("get content invalid.")
		return
	}
	//5. delete file
	_, e = f.Delete()
	if e != nil {
		t.Error(e)
		return
	}
}

func getCurrentPath() string {
	s, _ := exec.LookPath(os.Args[0])
	i := strings.LastIndex(s, "/")
	path := string(s[0 : i+1])
	return path
}
