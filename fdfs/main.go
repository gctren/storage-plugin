package main

import (
	"strconv"
	"time"

	. "github.com/ctripcorp/nephele/storage"
)

func main() {}

func New(config map[string]string) Storage {
	port, _ := strconv.Atoi(config["port"])
	socketPoolSize, _ := strconv.Atoi(config["socketPoolSize"])
	socketInitSize, _ := strconv.Atoi(config["socketInitSize"])
	connectTimeout, _ := strconv.Atoi(config["connectTimeout"])
	socketIdleTime, _ := strconv.Atoi(config["socketIdleTime"])
	ioTimeout, _ := strconv.Atoi(config["ioTimeout"])
	return &storage{
		host:           config["host"],
		port:           port,
		socketPoolSize: socketPoolSize,
		socketInitSize: socketInitSize,
		connectTimeout: time.Duration(connectTimeout) * time.Second,
		socketIdleTime: time.Duration(socketIdleTime) * time.Second,
		ioTimeout:      time.Duration(ioTimeout) * time.Second,
	}
}
