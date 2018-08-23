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
	keys     []string
	duration time.Duration
}

type getsResult struct {
	duration time.Duration
}

type deletesResult struct {
	duration time.Duration
}

func runPuts(kv KVAPI, duration time.Duration, valueLen int, putsResultCh chan putsResult) {
	var keys []string
	value := make([]byte, valueLen)
	for i := range value {
		value[i] = 'b'
	}

	doneCh := time.After(duration)
	go func() {
		t1 := time.Now()
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
		t2 := time.Now()
		putsResultCh <- putsResult{keys, t2.Sub(t1)}
	}()
}

func runGets(kv KVAPI, keys []string, valueLen int, getsResultCh chan getsResult) {
	value := make([]byte, valueLen)
	go func() {
		t1 := time.Now()
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
		t2 := time.Now()
		getsResultCh <- getsResult{t2.Sub(t1)}
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

	putsResultCh := make(chan putsResult)
	for i := 0; i < threadCount; i++ {
		go runPuts(kv, time.Duration(duration)*time.Second, size, putsResultCh)
	}

	var putsResults []putsResult
	for i := 0; i < threadCount; i++ {
		putsResults = append(putsResults, <-putsResultCh)
	}

	getsResultCh := make(chan getsResult)
	for i := 0; i < threadCount; i++ {
		go runGets(kv, putsResults[i].keys, size, getsResultCh)
	}
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

	totalKeys := float64(0)
	for i := 0; i < threadCount; i++ {
		totalKeys += float64(len(putsResults[i].keys))
	}

	fmt.Println("total keys:", totalKeys)
	fmt.Println("puts/sec", totalKeys/float64(duration), "puts bandwidth:", humanize.Bytes(uint64((totalKeys*float64(size))/float64(duration))), "/sec")
	var delta time.Duration
	for i := 0; i < threadCount; i++ {
		delta += getsResults[i].duration
	}
	deltaSecs := delta.Seconds() / float64(threadCount)
	fmt.Println("gets/sec", totalKeys/deltaSecs, "gets bandwidth", humanize.Bytes(uint64((totalKeys*float64(size))/deltaSecs)), "/sec")
	delta = time.Duration(0)
	for i := 0; i < threadCount; i++ {
		delta += deletesResults[i].duration
	}
	deltaSecs = delta.Seconds() / float64(threadCount)
	fmt.Println("deletes/sec", totalKeys/delta.Seconds())
}
