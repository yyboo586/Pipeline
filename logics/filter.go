package logics

import (
	"Pipeline/interfaces"
	"context"
	"sync"
)

var (
	filterOnce     sync.Once
	filterInstance *LogicsFiletr
)

type LogicsFiletr struct {
	bufferSize int64
	bufferChan chan map[string]interface{}

	next interfaces.Processor
}

func NewLogicsFilter() interfaces.Processor {
	filterOnce.Do(func() {
		filterInstance = &LogicsFiletr{
			bufferSize: 1000000,

			next: nil,
		}
		filterInstance.bufferChan = make(chan map[string]interface{}, filterInstance.bufferSize)

		go filterInstance.process()
	})

	return filterInstance
}

func (f *LogicsFiletr) Process(ctx context.Context, data map[string]interface{}) {
	f.bufferChan <- data
}

func (f *LogicsFiletr) process() {
	ctx := context.Background()

	for data := range f.bufferChan {
		if data["bizid"].(int) <= 25 {
			continue
		}

		if f.next != nil {
			f.next.Process(ctx, data)
		}
	}
}

func (f *LogicsFiletr) SetNext(_ context.Context, ps interfaces.Processor) {
	f.next = ps
}
