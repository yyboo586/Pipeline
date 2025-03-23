package interfaces

import (
	"context"
)

type Processor interface {
	Process(context.Context, map[string]interface{})
	SetNext(context.Context, Processor)
}
