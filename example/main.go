package main

import (
	"fmt"
	. "github.com/ctripcorp/nephele/storage"
	"plugin"
)

var New func(config map[string]string) Storage

func init() {
	oss, err := plugin.Open("storage.so")
	if err != nil {
		panic(err)
	}
	symbol, err := oss.Lookup("New")
	if err != nil {
		panic(err)
	}
	New = symbol.(func(config map[string]string) Storage)
}

func main() {
	s := New(map[string]string{
		"endpoint":        "[endpoint]",
		"bucketname":      "[bucketname]",
		"accessKeyId":     "[accessKeyId]",
		"accessKeySecret": "[accessKeySecret]",
	})

	fmt.Println(s.File("[filename]").Bytes())
	fmt.Println(s.Iterator("[prefix]", "[lastKey]").Next())
}
