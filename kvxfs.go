package main

import (
	"io"
	"os"
	"path"
)

type kvxfs struct {
	dir string
}

func newKVXFS(device string) (*kvxfs, error) {
	return &kvxfs{device}, nil
}

func (k *kvxfs) Put(key string, value []byte) error {
	f, err := os.OpenFile(path.Join(k.dir, key), os.O_CREATE|os.O_WRONLY|os.O_SYNC, 0400)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(value)
	return err
}

func (k *kvxfs) Get(key string, value []byte) error {
	f, err := os.OpenFile(path.Join(k.dir, key), os.O_SYNC|os.O_RDONLY, 0400)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.ReadFull(f, value)
	return err
}

func (k *kvxfs) Delete(key string) error {
	return os.Remove(path.Join(k.dir, key))
}
