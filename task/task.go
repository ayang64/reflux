package task

import (
	"context"
	"time"
)

type Task struct {
	ID     int
	Name   string
	Start  time.Time
	cancel func()
}

func (t *Task) run(ctx context.Context, f func(context.Context) error) error {
	ctx, cancel := context.WithCancel(ctx)
	t.cancel = cancel
	t.Start = time.Now()
	return f(ctx)
}
