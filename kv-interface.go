package main

type KVAPI interface {
	Put(key string, value []byte) error
	Get(key string, value []byte) error
	Delete(key string) error
}
