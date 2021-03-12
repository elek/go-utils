package kv

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"time"
)

type DirKV struct {
	Path string
}


func (dir *DirKV) Put(key string, value []byte) error {
	file := path.Join(dir.Path, key)
	_ = os.MkdirAll(path.Dir(file), 0755)
	return ioutil.WriteFile(file, value, 0644)
}

func (dir *DirKV) List(prefix string) ([]string, error) {
	prefixDir := path.Join(dir.Path, prefix)
	fileInfos, err := ioutil.ReadDir(prefixDir)
	if err != nil {
		return nil, nil
	}

	result := make([]string, 0)
	for _, fileInfo := range fileInfos {
		result = append(result, path.Join(prefix, fileInfo.Name()))
	}
	return result, nil
}

func (dir *DirKV) Contains(key string) bool {
	file := path.Join(dir.Path, key)
	_, err := os.Stat(file)
	if os.IsNotExist(err) {
		return false
	} else if err == nil {
		return true
	} else {
		return false
	}

}

func (dir *DirKV) Get(prefix string) ([]byte, error) {
	ret, err := ioutil.ReadFile(path.Join(dir.Path, prefix))
	return ret, err
}

func (dir *DirKV) GetReader(prefix string) (io.Reader, error) {
	return os.Open(path.Join(dir.Path, prefix))
}

func (dir *DirKV) GetOrDefault(key string, defaultFunc Getter) ([]byte, error) {
	if !dir.Contains(key) {
		val, err := defaultFunc(key)
		if err != nil {
			return nil, err
		}
		err = dir.Put(key, val)
		if err != nil {
			return nil, err
		}
	}
	return dir.Get(key)
}

func (dir *DirKV) IsChanged(since time.Time, prefix string) (bool, error) {
	stat, err := os.Stat(path.Join(dir.Path, prefix))
	if err != nil {
		return true, nil
	}
	return stat.ModTime().After(since), nil
}

func (dir *DirKV) IterateAll(action IteratorAction) error {
	return dir.IterateSubTree("", action)
}

func (dir *DirKV) IterateValues(prefix string, action KeyValueIteratorAction) error {
	files, err := ioutil.ReadDir(path.Join(dir.Path, prefix))
	if err != nil {
		return err
	}
	for _, file := range files {
		value, err := dir.Get(prefix)
		if err != nil {
			return err
		}
		err = action(path.Join(prefix, file.Name()), value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (dir *DirKV) Iterate(prefix string, action IteratorAction) error {
	files, err := ioutil.ReadDir(path.Join(dir.Path, prefix))
	if err != nil {
		return err
	}
	for _, file := range files {
		err = action(path.Join(prefix, file.Name()))
		if err != nil {
			return err
		}
	}
	return nil
}

func (dir *DirKV) IterateSubTree(prefix string, action IteratorAction) error {
	return filepath.Walk(path.Join(dir.Path, prefix),
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if dir.Path == path || info.IsDir() {
				return nil
			}
			return action(path[len(dir.Path)+1:])
		})
}

func (dir *DirKV) Close() error {
	return nil
}
