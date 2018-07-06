package main

import (
	"fmt"
	"sync"
	"time"
)

type client interface {
	// download to buffer
	DownloadToBuffer(fileId string) ([]byte, error)

	//DownloadToBufferByOffset download to buffer by offset
	DownloadToBufferByOffset(fileId string, offset, size int64) ([]byte, error)

	// upload by buffer
	UploadByBuffer(groupName string, filebuffer []byte, fileExtName string) (string, error)

	// upload appender
	UploadAppenderByBuffer(groupName string, filebuffer []byte, fileExtName string) (string, error)

	// append file
	AppendFile(fileBuffer []byte, appenderFileId string) error

	// upload slave by buffer
	UploadSlaveByBuffer(filebuffer []byte, remoteFileId string, prefixName string, fileExtName string) (string, error)

	// delete file
	DeleteFile(remoteFileId string) error
}

// ClientConfig
type clientConfig struct {
	SocketPoolSize int

	SocketInitSize int

	ConnectTimeout time.Duration

	SocketIdleTime time.Duration

	IoTimeout time.Duration
}

type fdfsClient struct {
	// client config
	config *clientConfig

	// // clusterName
	// clusterName string

	//tracker client containing a connetction pool
	tracker *trackerClient

	//storage client map
	storages *safeMap

	//use to read or write a storage client from map
	mutex sync.RWMutex
}

// var clients = NewSafeMap()
// var mutex = sync.Mutex{}

//NewFdfsClient create a connection pool to a tracker
//the tracker is selected randomly from tracker group
func newfdfsClient(trackerHost string, trackerPort int, config *clientConfig) (client, error) {
	tc, err := newTrackerClient(trackerHost, trackerPort, config)
	if err != nil {
		return nil, err
	}
	return &fdfsClient{tracker: tc, storages: newsafeMap(), config: config}, nil
}

func newfdfsClient1(trackerHost string, trackerPort int) (client, error) {
	conf := &clientConfig{SocketIdleTime: 100 * time.Second,
		SocketInitSize: 3,
		SocketPoolSize: 3,
		ConnectTimeout: 3 * time.Second,
		IoTimeout:      3 * time.Second}
	tc, err := newTrackerClient(trackerHost, trackerPort, conf)
	if err != nil {
		return nil, err
	}
	return &fdfsClient{tracker: tc, storages: newsafeMap(), config: conf}, nil
}

// DownloadToBuffer
func (this *fdfsClient) DownloadToBuffer(fileId string) ([]byte, error) {
	return this.DownloadToBufferByOffset(fileId, 0, 0)
}

func (this *fdfsClient) DownloadToBufferByOffset(fileId string, offset, size int64) ([]byte, error) {
	return this.downloadToBufferByOffset(fileId, offset, size)
}

func (this *fdfsClient) AppendFile(fileBuffer []byte, appenderFileId string) error {

	//split file id to two parts: group name and file name
	groupName, appenderFileName, err := splitFileId(appenderFileId)
	if err != nil {
		return err
	}
	//query a upload server from tracker
	storeInfo, err := this.tracker.trackerQueryStorageUpdate(groupName, appenderFileName)
	if err != nil {
		return err
	}
	//get a storage client from storage map, if not exist, create a new storage client
	storeClient, err := this.getStorage(storeInfo.ipAddr, storeInfo.port)
	if err != nil {
		return err
	}
	return storeClient.storageAppendFile(storeInfo, fileBuffer, appenderFileName)
}

func (this *fdfsClient) UploadAppenderByBuffer(groupName string, filebuffer []byte,
	fileExtName string) (string, error) {

	return this.upload(groupName, filebuffer, fileExtName, STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE)
}

func (this *fdfsClient) UploadByBuffer(groupName string, filebuffer []byte,
	fileExtName string) (string, error) {

	return this.upload(groupName, filebuffer, fileExtName, STORAGE_PROTO_CMD_UPLOAD_FILE)
}

// UploadByBuffer
func (this *fdfsClient) upload(groupName string, filebuffer []byte,
	fileExtName string, cmd int) (string, error) {
	//query a upload server from tracker
	storeInfo, err := this.tracker.queryStroageStoreWithGroup(groupName)
	if err != nil {
		return "", err
	}
	//get a storage client from storage map, if not exist, create a new storage client
	storeClient, err := this.getStorage(storeInfo.ipAddr, storeInfo.port)
	if err != nil {
		return "", err
	}
	return storeClient.storageUploadByBuffer(storeInfo, filebuffer, fileExtName, cmd)
}

// UploadSlaveByBuffer
func (this *fdfsClient) UploadSlaveByBuffer(fileBuffer []byte, fileId string,
	prefixName string, fileExtName string) (string, error) {
	groupName, fileName, err := splitFileId(fileId)
	if err != nil {
		return "", err
	}
	//query a upload server from tracker
	storeInfo, err := this.tracker.trackerQueryStorageUpdate(groupName, fileName)
	if err != nil {
		return "", err
	}
	//get a storage client from storage map, if not exist, create a new storage client
	storeClient, err := this.getStorage(storeInfo.ipAddr, storeInfo.port)
	if err != nil {
		return "", err
	}
	return storeClient.storageUploadSlaveByBuffer(storeInfo, fileBuffer, fileName, prefixName, fileExtName)
}

func (this *fdfsClient) DeleteFile(fileId string) error {
	groupName, fileName, err := splitFileId(fileId)
	if err != nil {
		return err
	}
	storeInfo, err := this.tracker.trackerQueryStorageUpdate(groupName, fileName)
	if err != nil {
		return err
	}
	//get a storage client from storage map, if not exist, create a new storage client
	storeClient, err := this.getStorage(storeInfo.ipAddr, storeInfo.port)
	if err != nil {
		return err
	}
	return storeClient.storageDeleteFile(storeInfo, fileName)
}

func (this *fdfsClient) downloadToBufferByOffset(fileId string, offset,
	downloadSize int64) ([]byte, error) {
	//split file id to two parts: group name and file name
	groupName, fileName, err := splitFileId(fileId)
	if err != nil {
		return nil, err
	}
	//query a download server from tracker
	storeInfo, err := this.tracker.trackerQueryStorageFetch(groupName, fileName)
	if err != nil {
		return nil, err
	}

	//get a storage client from storage map, if not exist, create a new storage client
	storeClient, err := this.getStorage(storeInfo.ipAddr, storeInfo.port)
	if err != nil {
		return nil, err
	}
	return storeClient.storageDownload(storeInfo, offset, downloadSize, fileName)
}

func (this *fdfsClient) getStorage(ip string, port int) (*storageClient, error) {
	storageKey := fmt.Sprintf("%s-%d", ip, port)
	//if the storage with the key exists, return the stroage
	//else create a new stroage and return
	var (
		val interface{}
		ok  bool
	)
	if val, ok = this.storages.Get(storageKey); !ok {
		if client, err := newStorageClient(ip, port, this.config); err != nil {
			return nil, err
		} else {
			val, _ = this.storages.SetIfNotExist(storageKey, client)
		}
	}
	return val.(*storageClient), nil
}
