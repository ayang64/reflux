package task

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

type Manager struct {
	mu sync.RWMutex
	id int
	m  map[int]*Task
}

func (m *Manager) String() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	b := &strings.Builder{}

	fmt.Fprintf(b, "%s\t%s\t%s\n", "task id", "duration", "name")
	fmt.Fprintf(b, "%s\t%s\t%s\n", "=======", "========", "====")
	for id, t := range m.m {
		fmt.Fprintf(b, "%d\t%s\t%s\n", id, time.Since(t.Start), t.Name)
	}

	return b.String()
}

func NewManager(opts ...func(*Manager) error) (*Manager, error) {
	m := Manager{
		m: map[int]*Task{},
	}
	for _, opt := range opts {
		if err := opt(&m); err != nil {
			return nil, err
		}
	}
	return &m, nil
}

func (m *Manager) newTask(name string) *Task {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := Task{
		ID:   m.id,
		Name: name,
	}
	m.id++
	return &t
}

func (m *Manager) NewTask(name string) *Task {

	return m.newTask(name)
}

func (m *Manager) getTask(id int) (*Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, exists := m.m[id]
	if !exists {
		return nil, fmt.Errorf("no such task id %d", id)
	}
	return t, nil
}

func (m *Manager) Kill(id int) error {
	t, err := m.getTask(id)
	if err != nil {
		return fmt.Errorf("could not kill task: %w", err)
	}

	t.cancel()

	return nil
}

func (m *Manager) Run(ctx context.Context, name string, f func(context.Context) error) error {
	return func() error {
		t := m.NewTask(name)
		m.addTask(t)

		defer m.removeTask(t)

		return t.run(ctx, f)
	}()
}

func (m *Manager) removeTask(t *Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.m[t.ID]; !exists {
		return fmt.Errorf("attempting to remove non-existent task %d", t.ID)
	}
	delete(m.m, t.ID)
	return nil
}

func (m *Manager) addTask(t *Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.m[t.ID] = t

	return nil
}
