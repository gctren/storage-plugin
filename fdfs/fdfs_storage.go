package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"path/filepath"
)

type storageClient struct {
	host   string
	port   int
	config *clientConfig
	pool
}

func newStorageClient(host string, port int, config *clientConfig) (*storageClient, error) {
	c := &storageClient{host: host, port: port, config: config}
	p, e := newblockingPool(config.SocketInitSize, config.SocketPoolSize, config.SocketIdleTime, c.makeConn)
	if e != nil {
		return nil, e
	}
	c.pool = p
	return c, nil
}

func (this *storageClient) storageDownload(storeInfo *storageInfo, offset, downloadSize int64,
	fileName string) ([]byte, error) {
	//get a connetion from pool
	conn, err := getConnFromPool(this)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	//package length:file_offset(8)  download_bytes(8)  group_name(16)  file_name(n)
	buffer := newHeaderBuffer(STORAGE_PROTO_CMD_DOWNLOAD_FILE, 32+len(fileName))

	// offset
	binary.Write(buffer, binary.BigEndian, offset)
	// download bytes
	binary.Write(buffer, binary.BigEndian, downloadSize)
	// 16 bit groupName
	buffer.WriteString(fixString(storeInfo.groupName, FDFS_GROUP_NAME_MAX_LEN))
	// fileName
	buffer.WriteString(fileName)

	return interactiveWithServerWithRespLimit(conn, buffer, nil, 128*1024*1024, this.config.IoTimeout)
}

func (this *storageClient) storageDeleteFile(storeInfo *storageInfo, fileName string) error {
	//get a connetion from pool
	conn, err := getConnFromPool(this)
	if err != nil {
		return err
	}
	defer conn.Close()

	buffer := newHeaderBuffer(STORAGE_PROTO_CMD_DELETE_FILE, FDFS_GROUP_NAME_MAX_LEN+len(fileName))
	//16 bit groupName
	buffer.WriteString(fixString(storeInfo.groupName, FDFS_GROUP_NAME_MAX_LEN))
	// fileNameLen bit fileName
	buffer.WriteString(fileName)

	_, err = interactiveWithServer(conn, buffer, nil, this.config.IoTimeout)
	return err
}

func (this *storageClient) storageAppendFile(storeInfo *storageInfo, fileBuffer []byte, appenderFileName string) error {
	var (
		appenderFileNameLen = len(appenderFileName)
		fileLen             = len(fileBuffer)
	)
	//get a connetion from pool
	conn, err := getConnFromPool(this)
	if err != nil {
		return err
	}
	defer conn.Close()

	buffer := newHeaderBuffer(STORAGE_PROTO_CMD_APPEND_FILE, 16+appenderFileNameLen+fileLen)
	//8 bytes: appender filename length
	binary.Write(buffer, binary.BigEndian, int64(appenderFileNameLen))
	//8 bytes: file size
	binary.Write(buffer, binary.BigEndian, int64(fileLen))
	//appender file name
	buffer.WriteString(appenderFileName)

	_, err = interactiveWithServerWithRespLimit(conn, buffer, fileBuffer, 130, this.config.IoTimeout)
	return err
}

//stroage upload by buffer
func (this *storageClient) storageUploadByBuffer(storeInfo *storageInfo, fileBuffer []byte,
	fileExtName string, cmd int) (string, error) {
	return this.storageUploadFile(storeInfo, fileBuffer, int8(cmd), "", "", fileExtName)
}

//storage upload slave by buffer
func (this *storageClient) storageUploadSlaveByBuffer(storeInfo *storageInfo, fileBuffer []byte,
	remoteFileId string, prefixName string, fileExtName string) (string, error) {
	return this.storageUploadFile(storeInfo, fileBuffer, STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE, remoteFileId, prefixName, fileExtName)
}

//stroage upload file
func (this *storageClient) storageUploadFile(storeInfo *storageInfo, fileBuffer []byte, cmd int8,
	masterFileName string, prefixName string, fileExtName string) (string, error) {
	var (
		uploadSlave bool = false
		headerLen   int  = 15
		fileSize    int  = len(fileBuffer)
	)
	//get a connetion from pool
	conn, err := getConnFromPool(this)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	masterFilenameLen := len(masterFileName)
	if len(storeInfo.groupName) > 0 && len(masterFileName) > 0 {
		uploadSlave = true
		//master_len(8) file_size(8) prefix_name(16) file_ext_name(6) master_name(master_filename_len)
		headerLen = 38 + masterFilenameLen
	}

	buffer := newHeaderBuffer(cmd, headerLen+fileSize)
	if uploadSlave {
		// master file name len
		binary.Write(buffer, binary.BigEndian, int64(masterFilenameLen))
		// file size
		binary.Write(buffer, binary.BigEndian, int64(fileSize))
		// 16 bit prefixName
		buffer.WriteString(fixString(prefixName, FDFS_FILE_PREFIX_MAX_LEN))
		// 6 bit fileExtName
		buffer.WriteString(fixString(fileExtName, FDFS_FILE_EXT_NAME_MAX_LEN))
		// master_file_name
		buffer.WriteString(masterFileName)
	} else {
		//store_path_index
		buffer.WriteByte(byte(uint8(storeInfo.storePathIndex)))
		// file size
		binary.Write(buffer, binary.BigEndian, int64(fileSize))
		// 6 bit fileExtName
		buffer.WriteString(fixString(fileExtName, FDFS_FILE_EXT_NAME_MAX_LEN))
	}

	recvBuff, err := interactiveWithServerWithRespLimit(conn, buffer, fileBuffer, 130, this.config.IoTimeout)
	if err != nil {
		return "", err
	}
	// #recv_fmt |-group_name(16)-filename|
	if len(recvBuff) < FDFS_GROUP_NAME_MAX_LEN {
		return "", fmt.Errorf("recv package length %d != %d", len(recvBuff), FDFS_GROUP_NAME_MAX_LEN)
	}
	groupName := stripString(string(recvBuff[0:FDFS_GROUP_NAME_MAX_LEN]))
	remoteFilename := string(recvBuff[FDFS_GROUP_NAME_MAX_LEN:])
	return filepath.Join(groupName, remoteFilename), nil
}

//factory method used to dial
func (this *storageClient) makeConn() (net.Conn, error) {
	return net.DialTimeout("tcp", fmt.Sprintf("%s:%d", this.host, this.port), this.config.ConnectTimeout)
}
