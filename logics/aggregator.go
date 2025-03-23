package logics

import (
	"Pipeline/interfaces"
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sync"
	"time"
)

var (
	aggregatorOnce     sync.Once
	aggregatorInstance *Aggregator
)

const (
	maxItemValueKey   = "__value__max"
	minItemValueKey   = "__value__min"
	sumItemValueKey   = "__value__sum"
	countItemValueKey = "__value__count"
)

type Aggregator struct {
	interval time.Duration
	ticker   *time.Ticker

	bufferSize int
	bufferChan chan map[string]interface{}

	dataSlice  []map[string]interface{}
	resultChan chan []map[string]interface{}
	output     io.Writer

	errChan chan struct{}

	next interfaces.Processor

	totalCount int64
}

func NewAggregator() interfaces.Processor {
	aggregatorOnce.Do(func() {
		aggregatorInstance = &Aggregator{
			interval:   time.Second * 1,
			bufferSize: 100000,
			output:     os.Stdout,
		}
		aggregatorInstance.ticker = time.NewTicker(aggregatorInstance.interval)
		aggregatorInstance.bufferChan = make(chan map[string]interface{}, aggregatorInstance.bufferSize)
		aggregatorInstance.dataSlice = make([]map[string]interface{}, 0, aggregatorInstance.bufferSize)
		aggregatorInstance.resultChan = make(chan []map[string]interface{}, 1)
		aggregatorInstance.errChan = make(chan struct{}, 1)

		go aggregatorInstance.process()
		go aggregatorInstance.flush()
	})

	return aggregatorInstance
}

func (agg *Aggregator) Process(ctx context.Context, data map[string]interface{}) {
	agg.bufferChan <- data
}

func (agg *Aggregator) SetNext(_ context.Context, ps interfaces.Processor) {
	agg.next = ps
}

func (agg *Aggregator) process() {
	defer agg.ticker.Stop()

	for {
		select {
		case data := <-agg.bufferChan:
			agg.dataSlice = append(agg.dataSlice, data)
		case <-agg.ticker.C:
			agg.compute()
		case <-agg.errChan:
			log.Println("aggregator processor received exit signal, quit...")
			return
		}
	}
}

func (agg *Aggregator) compute() {
	length := len(agg.dataSlice)
	if length <= 0 {
		return
	}

	maxValue := math.SmallestNonzeroFloat64
	minValue := math.MaxFloat64
	var sumValue float64 = 0
	for _, item := range agg.dataSlice {
		v := item["__value__"].(float64)
		if v > maxValue {
			maxValue = v
		}
		if v < minValue {
			minValue = v
		}
		sumValue += v
	}

	max := deepCopy(agg.dataSlice[0])
	min := deepCopy(agg.dataSlice[0])
	sum := deepCopy(agg.dataSlice[0])
	count := deepCopy(agg.dataSlice[0])

	key := "__value__"
	delete(max, key)
	delete(min, key)
	delete(sum, key)
	delete(count, key)

	max[maxItemValueKey] = maxValue
	min[minItemValueKey] = minValue
	sum[sumItemValueKey] = sumValue
	count[countItemValueKey] = length
	agg.totalCount += int64(length)

	agg.dataSlice = agg.dataSlice[:0]
	agg.resultChan <- []map[string]interface{}{max, min, sum, count}
}

func deepCopy(data map[string]interface{}) map[string]interface{} {
	tmp := make(map[string]interface{}, len(data))
	for k, v := range data {
		tmp[k] = v
	}

	return tmp
}

func (agg *Aggregator) flush() {
	for {
		select {
		case result := <-agg.resultChan:
			// data, _ := json.Marshal(result)
			// agg.output.Write(data)
			for _, v := range result {
				fmt.Printf("\t%v\n", v)
			}
			fmt.Printf("\t总计处理记录数: %d\n\n", agg.totalCount)
		}
	}
}
