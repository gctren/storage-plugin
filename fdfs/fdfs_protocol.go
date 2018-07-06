package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"time"
)

type storageInfo struct {
	ipAddr         string
	port           int
	groupName      string
	storePathIndex int
}

type header struct {
	pkgLen int64
	cmd    int8
	status int8
}

// splitFileId split file id to group name and file name
func splitFileId(fileId string) (string, string, error) {
	s := strings.SplitN(fileId, "/", 2)
	if len(s) < 2 {
		return "", "", fmt.Errorf("fdfs fileid error.fileid:%s", fileId)
	}

	return s[0], s[1], nil
}

// parseStatusCode
func parseStatusCode(status int) string {
	switch status {
	case 2:
		return "FileNotExistWarning"
	case 22:
		return "InvalidArgumentWarning"
	default:
		return "StatusWarning"
	}
}

//read fdfs header
func recvHeader(conn net.Conn, timeout time.Duration) (*header, error) {
	data, err := tcpRecv(conn, 10, timeout)
	if err != nil {
		return nil, err
	}
	buff := bytes.NewBuffer(data)
	h := &header{}
	binary.Read(buff, binary.BigEndian, &h.pkgLen)
	if h.pkgLen < 0 {
		return nil, fmt.Errorf("recv package length %d != %d", int(h.pkgLen), 0)
	}
	cmd, _ := buff.ReadByte()
	status, _ := buff.ReadByte()
	if status != 0 {
		return nil, fmt.Errorf("receive status: %d != 0", int(status))
	}
	h.cmd = int8(cmd)
	h.status = int8(status)
	return h, nil
}

func recvResponse(conn net.Conn, timeout time.Duration) ([]byte, error) {
	//receive response header
	h, err := recvHeader(conn, timeout)
	if err != nil {
		return nil, err
	}
	if h.pkgLen == 0 {
		return nil, nil
	}
	//receive body
	recvBuff, err := tcpRecv(conn, h.pkgLen, timeout)
	if err != nil {
		return nil, err
	}
	return recvBuff, nil
}

func recvResponseWithLimit(conn net.Conn, maxPkgLen int64, timeout time.Duration) ([]byte, error) {
	//receive response header
	h, err := recvHeader(conn, timeout)
	if err != nil {
		return nil, err
	}
	if h.pkgLen > maxPkgLen {
		return nil, fmt.Errorf("recv package length %d != %d", int(h.pkgLen), int(maxPkgLen))
	}
	//receive body
	recvBuff, err := tcpRecv(conn, h.pkgLen, timeout)
	if err != nil {
		return nil, err
	}
	return recvBuff, nil
}
