package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/skyrings/skyring-common/tools/uuid"
)

type putsResult struct {
	keys []string
}

type getsResult struct {
	count int
}

type deletesResult struct {
	duration time.Duration
}

func runPuts(kv KVAPI, doneCh chan struct{}, valueLen int, putsResultCh chan putsResult) {
	var keys []string
	value := make([]byte, valueLen)
	for i := range value {
		value[i] = 'b'
	}

	go func() {
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
			case <-doneCh:
				break L
			default:
			}
		}
		putsResultCh <- putsResult{keys}
	}()
}

func runGets(kv KVAPI, doneCh chan struct{}, keys []string, valueLen int, getsResultCh chan getsResult) {
	value := make([]byte, valueLen)
	count := 0
	go func() {
	L:
		for {
			for i, key := range keys {
				err := kv.Get(key, value)
				if err != nil {
					log.Fatal(err)
				}
				if value[0] != 'b' {
					fmt.Println(i, key, "FAIL")
					log.Fatal("value mismatch")
				}
				count++
				value[0] = 0
				select {
				case <-doneCh:
					break L
				default:
				}
			}
		}
		getsResultCh <- getsResult{count}
	}()
}

func runDeletes(kv KVAPI, keys []string, deletesResultCh chan deletesResult) {
	go func() {
		t1 := time.Now()
		for _, key := range keys {
			err := kv.Delete(key)
			if err != nil {
				log.Fatal(err)
			}
		}
		t2 := time.Now()
		deletesResultCh <- deletesResult{t2.Sub(t1)}
	}()
}

func main() {
	fsPath_ := flag.String("p", "", "path")
	duration_ := flag.Int("d", 10, "duration")
	size_ := flag.Int("s", 2*1024*1024, "value size")
	threadCount_ := flag.Int("t", 1, "threads count")
	flag.Parse()

	threadCount := *threadCount_
	duration := *duration_
	size := *size_
	fsPath := *fsPath_

	if fsPath == "" {
		log.Fatal("path not specified")
	}

	var kv KVAPI
	var err error
	if strings.HasPrefix(fsPath, "/dev/") {
		kv, err = newKVSSD(fsPath)
	} else {
		kv, err = newKVXFS(fsPath)
	}
	if err != nil {
		log.Fatal(err)
	}

	doneCh := make(chan struct{})
	putsResultCh := make(chan putsResult)
	for i := 0; i < threadCount; i++ {
		go runPuts(kv, doneCh, size, putsResultCh)
	}

	<-time.After(time.Duration(duration) * time.Second)
	close(doneCh)

	var putsResults []putsResult
	for i := 0; i < threadCount; i++ {
		putsResults = append(putsResults, <-putsResultCh)
	}

	totalKeys := float64(0)
	for i := 0; i < threadCount; i++ {
		totalKeys += float64(len(putsResults[i].keys))
	}

	fmt.Println("total keys:", totalKeys)
	fmt.Println("puts/sec", totalKeys/float64(duration), "puts bandwidth:", humanize.Bytes(uint64((totalKeys*float64(size))/float64(duration))), "/sec")

	doneCh = make(chan struct{})
	getsResultCh := make(chan getsResult)
	for i := 0; i < threadCount; i++ {
		go runGets(kv, doneCh, putsResults[i].keys, size, getsResultCh)
	}

	<-time.After(time.Duration(duration) * time.Second)
	close(doneCh)

	var getsResults []getsResult
	for i := 0; i < threadCount; i++ {
		getsResults = append(getsResults, <-getsResultCh)
	}

	deletesResultCh := make(chan deletesResult)
	for i := 0; i < threadCount; i++ {
		go runDeletes(kv, putsResults[i].keys, deletesResultCh)
	}
	var deletesResults []deletesResult
	for i := 0; i < threadCount; i++ {
		deletesResults = append(deletesResults, <-deletesResultCh)
	}

	totalKeys = float64(0)
	for i := 0; i < threadCount; i++ {
		totalKeys += float64(getsResults[i].count)
	}
	fmt.Println("gets/sec", totalKeys/float64(duration), "gets bandwidth:", humanize.Bytes(uint64((totalKeys*float64(size))/float64(duration))), "/sec")
}
