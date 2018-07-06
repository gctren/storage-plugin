package main

import (
	"encoding/binary"
	"fmt"
	"net"
)

type trackerClient struct {
	host   string
	port   int
	config *clientConfig
	pool
}

func newTrackerClient(host string, port int, config *clientConfig) (client *trackerClient, err error) {
	c := &trackerClient{host: host, port: port, config: config}
	if p, e := newblockingPool(config.SocketInitSize, config.SocketPoolSize,
		config.SocketIdleTime, c.makeConn); e != nil {
		err = e
	} else {
		c.pool = p
		client = c
	}
	return
}

//query upload storage with group name
func (this *trackerClient) queryStroageStoreWithGroup(groupName string) (info *storageInfo, err error) {
	//get a connection from pool
	conn, err := getConnFromPool(this)
	if err != nil {
		return
	}
	defer conn.Close()
	buffer := newHeaderBuffer(TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ONE, FDFS_GROUP_NAME_MAX_LEN)
	//16 bit groupName
	buffer.WriteString(fixString(groupName, FDFS_GROUP_NAME_MAX_LEN))

	recvBuff, err := interactiveWithServer(conn, buffer, nil, this.config.IoTimeout)
	if err != nil {
		return
	}
	// #recv_fmt |-group_name(16)-ipaddr(16-1)-port(8)-store_path_index(1)|
	if len(recvBuff) != TRACKER_QUERY_STORAGE_STORE_BODY_LEN {
		err = fmt.Errorf("recv package length %d != %d", len(recvBuff), TRACKER_QUERY_STORAGE_STORE_BODY_LEN)
		return
	}
	info = castStorageInfo(recvBuff)

	return
}

func (this *trackerClient) trackerQueryStorageUpdate(groupName string, remoteFilename string) (*storageInfo, error) {
	return this.trackerQueryStorage(groupName, remoteFilename, TRACKER_PROTO_CMD_SERVICE_QUERY_UPDATE)
}

//fetch a  download stroage from tracker
func (this *trackerClient) trackerQueryStorageFetch(groupName string, fileName string) (*storageInfo, error) {
	return this.trackerQueryStorage(groupName, fileName, TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE)
}

//query stroage sever with specific command
func (this *trackerClient) trackerQueryStorage(groupName string, fileName string,
	cmd int8) (info *storageInfo, err error) {
	//get a connection from pool
	var conn net.Conn
	conn, err = getConnFromPool(this)
	if err != nil {
		return
	}
	defer conn.Close()

	buffer := newHeaderBuffer(cmd, FDFS_GROUP_NAME_MAX_LEN+len(fileName))
	//16 bit groupName
	buffer.WriteString(fixString(groupName, FDFS_GROUP_NAME_MAX_LEN))
	// fileName
	buffer.WriteString(fileName)
	var recvBuff []byte
	recvBuff, err = interactiveWithServer(conn, buffer, nil, this.config.IoTimeout)
	if err != nil {
		return
	}
	// #recv_fmt |-group_name(16)-ipaddr(16-1)-port(8)|
	if len(recvBuff) != TRACKER_QUERY_STORAGE_FETCH_BODY_LEN {
		err = fmt.Errorf("recv package length %d != %d", len(recvBuff), TRACKER_QUERY_STORAGE_FETCH_BODY_LEN)
		return
	}
	info = castStorageInfo(recvBuff)

	return
}

// group_name(16)-ipaddr(16-1)-port(8)-store_path_index(1)
func castStorageInfo(b []byte) *storageInfo {
	info := &storageInfo{}
	info.groupName = stripString(string(b[:16]))
	info.ipAddr = stripString(string(b[16:31]))
	info.port = int(binary.BigEndian.Uint64(b[31:39]))
	if len(b) == TRACKER_QUERY_STORAGE_STORE_BODY_LEN {
		info.storePathIndex = int(b[39])
	}
	return info
}

//factory method used for dial
func (this *trackerClient) makeConn() (net.Conn, error) {
	return net.DialTimeout("tcp", fmt.Sprintf("%s:%d", this.host, this.port), this.config.ConnectTimeout)
}
