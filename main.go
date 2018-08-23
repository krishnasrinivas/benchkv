package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/skyrings/skyring-common/tools/uuid"
)

var keys []string

func runPuts(kv KVAPI, ch chan struct{}, value []byte) {
L:
	for {
		keyuuid, err := uuid.New()
		if err != nil {
			log.Fatal(err)
		}
		key := keyuuid.String()[:16]
		err = kv.Put(key, value)
		if err != nil {
			log.Fatal(err)
		}
		keys = append(keys, key)
		select {
		case <-ch:
			break L
		default:
		}
	}
}

func runGets(kv KVAPI, value []byte) {
	for i, key := range keys {
		err := kv.Get(key, value)
		if err != nil {
			log.Fatal(err)
		}
		if value[0] != 'b' {
			fmt.Println(i, key, "FAIL")
			log.Fatal("value mismatch")
		}
		value[0] = 0
	}
}

func runDeletes(kv KVAPI) {
	for _, key := range keys {
		err := kv.Delete(key)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func main() {
	fsPath := flag.String("p", "", "path")
	duration := flag.Int("d", 10, "duration")
	size := flag.Int("s", 2*1024*1024, "value size")
	flag.Parse()

	if *fsPath == "" {
		log.Fatal("path not specified")
	}
	var kv KVAPI
	var err error
	if strings.HasPrefix(*fsPath, "/dev/") {
		kv, err = newKVSSD(*fsPath)
	} else {
		kv, err = newKVXFS(*fsPath)
	}
	if err != nil {
		log.Fatal(err)
	}

	value := make([]byte, *size)
	for i := range value {
		value[i] = 'b'
	}

	ch := make(chan struct{})
	go runPuts(kv, ch, value)
	<-time.After(time.Duration(*duration) * time.Second)
	close(ch)
	t1 := time.Now()
	runGets(kv, value)
	t2 := time.Now()
	runDeletes(kv)
	t3 := time.Now()
	fmt.Println("number of keys", len(keys))
	fmt.Println("puts/sec", len(keys) / *duration)
	delta := t2.Sub(t1).Seconds()
	fmt.Println("gets/sec", float64(len(keys))/delta)
	delta = t3.Sub(t2).Seconds()
	fmt.Println("deletes/sec", float64(len(keys))/delta)
}
