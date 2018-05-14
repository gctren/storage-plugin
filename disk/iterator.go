package main

import (
	"errors"
	"io/ioutil"
	"os"
	"path"
	"strings"

	. "github.com/ctripcorp/nephele/storage"
)

type iterator struct {
	dir     string
	lastKey string
}

func (iter *iterator) Next() (File, error) {
	iter.dir = join(iter.dir, "")
	fi, dir, err := getNextFile(iter.dir, iter.lastKey)
	if err != nil {
		return nil, err
	}
	if fi == nil {
		return nil, errors.New("no files.")
	}
	return &file{dir: iter.dir, key: join(strings.TrimPrefix(dir, iter.dir), fi.Name())}, nil
}

func getNextFile(root, key string) (os.FileInfo, string, error) {
	root = join(root, "")
	filePath, err := getRealPath(join(root, key))
	dir := join(path.Dir(filePath), "")
	if err != nil {
		return nil, dir, err
	}

	if len(root) > len(filePath) {
		return nil, dir, errors.New("not file.")
	}
	if filePath != join(root, key) {
		dirs := strings.Split(filePath, "/")
		dir = join(strings.Join(dirs[:len(dirs)-2], "/"), "")
	}
	fs, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, strings.TrimPrefix(filePath, root), err
	}
	isNext := dir == filePath
	for _, f := range fs {
		name := f.Name()
		if f.IsDir() {
			name = name + "/"
		}
		if join(dir, name) == filePath {
			isNext = true
			continue
		}
		if join(dir, name) != filePath && !isNext {
			continue
		}
		if f.IsDir() {
			fi, childDir, err := getChildFile(join(dir, name))
			if err != nil {
				return nil, childDir, err
			}
			if fi != nil {
				return fi, childDir, nil
			}
		} else {
			return f, dir, nil
		}
	}
	return getNextFile(root, strings.TrimPrefix(dir, root)+"noexists.test")
}

func getRealPath(path string) (string, error) {
	if path == "" || path == "/" {
		return "", errors.New("dir is not exists.")
	}
	exists, _, err := pathExists(path)
	if err != nil {
		return path, err
	}
	if exists {
		return path, nil
	}
	if strings.HasSuffix(path, "/") {
		dirs := strings.Split(path, "/")
		upperDir := join(strings.Join(dirs[:len(dirs)-2], "/"), "")
		return getRealPath(upperDir)
	}
	dirs := strings.Split(path, "/")
	upperDir := join(strings.Join(dirs[:len(dirs)-1], "/"), "")
	return getRealPath(upperDir)
}

func getChildFile(dir string) (os.FileInfo, string, error) {
	fs, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, dir, err
	}
	for _, f := range fs {
		if !f.IsDir() {
			return f, dir, nil
		}
		fi, childDir, err := getChildFile(join(dir, f.Name()+"/"))
		if err != nil {
			return nil, childDir, err
		}
		if fi != nil {
			return fi, childDir, nil
		}
	}
	return nil, dir, nil
}

func (iter *iterator) LastKey() string {
	return ""
}

func pathExists(path string) (bool, os.FileInfo, error) {
	fi, err := os.Stat(path)
	if err == nil {
		return true, fi, nil
	}
	if os.IsNotExist(err) {
		return false, fi, nil
	}
	return false, fi, err
}
