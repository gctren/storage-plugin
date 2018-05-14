package main

import . "github.com/ctripcorp/nephele/storage"

func main() {}

func New(config map[string]string) Storage {
	dir := config["dir"]

	return &storage{
		dir: dir,
	}
}
