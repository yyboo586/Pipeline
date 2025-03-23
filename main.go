package main

import (
	"Pipeline/interfaces"
	"Pipeline/logics"
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	_ "net/http/pprof"
)

func main() {
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()

	filter := logics.NewLogicsFilter()
	fill := logics.NewFill()
	agg := logics.NewAggregator()

	filter.SetNext(context.Background(), fill)
	fill.SetNext(context.Background(), agg)

	Test2(filter)
	fmt.Println("main goroutine done")
	time.Sleep(time.Second * 90)
}

func Test1(ps interfaces.Processor) {
	dataSlice := []map[string]interface{}{
		{
			"zone":       "gz",
			"bizid":      25,
			"env":        "dev",
			"os_type":    "linux",
			"os_version": "22.04",
			"__value__":  float64(1024),
		},
		{
			"zone":       "gz",
			"bizid":      26,
			"env":        "dev",
			"os_type":    "linux",
			"os_version": "22.04",
			"__value__":  float64(1024),
		},
	}
	for _, data := range dataSlice {
		ps.Process(context.Background(), data)
	}
}

func Test2(ps interfaces.Processor) {
	ctx := context.Background()
	numGoroutines := 10
	// 总计一亿条记录，每个Goroutine生产一千万条记录。
	// numRecords := 100000000
	for i := 0; i < numGoroutines; i++ {
		go func(i int) {
			for j := i * 10000000; j < (i+1)*10000000; j++ {
				data := map[string]interface{}{
					"zone":       "gz",
					"bizid":      getBIZID(j),
					"env":        "dev",
					"os_type":    "linux",
					"os_version": "22.04",
					"__value__":  rand.Float64() + 100,
				}

				ps.Process(ctx, data)
			}
		}(i)
	}
}

func getBIZID(i int) int {
	if i%2 == 0 {
		return rand.Intn(74) + 26
	} else {
		return rand.Intn(25)
	}
}
