package logics

import (
	"Pipeline/interfaces"
	"context"
	"os"
	"regexp"
	"runtime"
	"sync"
)

var (
	fillOnce     sync.Once
	fillInstance *LogicsFill
)

type LogicsFill struct {
	fieldsMap map[string]interface{} // 每条数据要补充的字段：如操作系统类型、版本

	bufferSize int64
	bufferChan chan map[string]interface{}

	next interfaces.Processor
}

func NewFill() interfaces.Processor {
	fillOnce.Do(func() {
		fillInstance = &LogicsFill{
			fieldsMap: map[string]interface{}{
				"os_type":    runtime.GOOS,
				"os_version": getOSVersion(),
			},
			bufferSize: 100000,

			next: nil,
		}
		fillInstance.bufferChan = make(chan map[string]interface{}, fillInstance.bufferSize)

		go fillInstance.process()
	})

	return fillInstance
}

func (f *LogicsFill) Process(ctx context.Context, data map[string]interface{}) {
	f.bufferChan <- data
}

func (f *LogicsFill) process() {
	ctx := context.Background()

	for data := range f.bufferChan {
		for k, v := range f.fieldsMap {
			data[k] = v
		}

		if f.next != nil {
			f.next.Process(ctx, data)
		}
	}
}

func (f *LogicsFill) SetNext(_ context.Context, ps interfaces.Processor) {
	f.next = ps
}

func getOSVersion() string {
	if runtime.GOOS != "linux" {
		return "not linux system"
	}

	data, err := os.ReadFile("/etc/os-release")
	if err != nil {
		return "Unknown Linux system"
	}

	re := regexp.MustCompile(`VERSION="(.*?)"`)
	match := re.FindStringSubmatch(string(data))
	if len(match) > 1 {
		return match[1]
	}
	return "Unknown Linux version"
}
