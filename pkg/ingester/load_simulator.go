package ingester

import (
	"fmt"
	"net/http"

	"go.uber.org/atomic"
)

var (
	moreLoad  = make(chan chan int64)
	lessLoad  = make(chan chan int64)
	totalLoad = atomic.NewInt64(0)
)

func MoreLoadHandler(w http.ResponseWriter, _ *http.Request) {
	load := make(chan int64)
	moreLoad <- load
	currentLoad := <-load
	w.Write([]byte("Load created. Total running: " + string(currentLoad)))
}

func LessLoadHandler(w http.ResponseWriter, _ *http.Request) {
	load := make(chan int64)
	lessLoad <- load
	currentLoad := <-load
	w.Write([]byte("Load stopped. Total running: " + string(currentLoad)))
}

func init() {
	go func() {
		for load := range moreLoad {
			go func(createdLoad chan int64) {
				totalLoad.Inc()
				createdLoad <- totalLoad.Load()
				for {
					select {
					case stoppedLoad := <-lessLoad:
						totalLoad.Dec()
						stoppedLoad <- totalLoad.Load()
						return
					default:
						n := fibonacci(40)
						fmt.Println(n)
					}
				}
			}(load)
		}
	}()
}

func fibonacci(n int) int {
	if n == 0 {
		return 0
	}
	if n == 1 || n == 2 {
		return 1
	}
	return fibonacci(n-1) + fibonacci(n-2)
}
