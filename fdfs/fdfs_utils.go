package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"time"
)

func getFileExt(filename string) string {
	parts := strings.Split(filename, ".")
	if len(parts) >= 2 {
		return parts[len(parts)-1]
	}
	return ""
}

func tcpSend(conn net.Conn, bytesStream []byte, timeout time.Duration) error {
	if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	if _, err := conn.Write(bytesStream); err != nil {
		return err
	}
	return nil
}

func tcpRecv(conn net.Conn, bufferSize int64, timeout time.Duration) ([]byte, error) {
	buff := make([]byte, bufferSize)
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(conn, buff); err != nil {
		return nil, err
	}
	return buff, nil
}

func stripString(s string) string {
	if i := strings.IndexByte(s, 0); i != -1 {
		return s[:i]
	} else {
		return s
	}
}

func fixString(s string, fix_length int) string {
	buff := make([]byte, fix_length)
	l := len(s)
	if l > fix_length {
		l = fix_length
	}
	for i := 0; i < l; i++ {
		buff[i] = s[i]
	}
	for i := l; i < fix_length; i++ {
		buff[i] = 0
	}
	return string(buff)
}

func interactiveWithServer(conn net.Conn, header *bytes.Buffer, body []byte, timeout time.Duration) (recv []byte, err error) {
	//send header
	if err = tcpSend(conn, header.Bytes(), timeout); err != nil {
		return
	}
	//send body
	if body != nil {
		if err = tcpSend(conn, body, timeout); err != nil {
			return
		}
	}
	//receive server response
	recv, err = recvResponse(conn, timeout)
	return

}

func interactiveWithServerWithRespLimit(conn net.Conn, header *bytes.Buffer, body []byte, maxPkgLen int64, timeout time.Duration) (recv []byte, err error) {
	//send header
	if err = tcpSend(conn, header.Bytes(), timeout); err != nil {
		return
	}
	//send body
	if body != nil {
		if err = tcpSend(conn, body, timeout); err != nil {
			return
		}
	}
	//receive server response
	recv, err = recvResponseWithLimit(conn, maxPkgLen, timeout)
	return
}

type connGetter interface {
	Get() (net.Conn, error)
}

func getConnFromPool(getter connGetter) (net.Conn, error) {
	conn, err := getter.Get()
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func newHeaderBuffer(cmd int8, pkgLen int) *bytes.Buffer {
	//request header
	buffer := new(bytes.Buffer)
	//package length
	binary.Write(buffer, binary.BigEndian, int64(pkgLen))
	//cmd
	buffer.WriteByte(byte(cmd))
	//status
	buffer.WriteByte(byte(0))

	return buffer
}
