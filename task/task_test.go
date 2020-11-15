package task

import (
	"context"
	"math/rand"
	"testing"
	"time"
)

func TestTask(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	taskName := func() string {
		words := []string{
			"angry", "cantelope", "dead", "duck", "funky",
			"hat", "lazy", "mighty", "mouse", "ocelot",
			"ordinary", "resilliant", "super", "turkey",
			"hippo", "vagrant", "warrior", "goose",
		}
		rnd := func() string {
			return words[rand.Intn(len(words))]
		}
		return rnd() + "-" + rnd() + "-" + rnd()
	}

	mgr, err := NewManager()
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error)

	ntasks := rand.Intn(20) + 5
	for i := 0; i < ntasks; i++ {
		go func() {
			errCh <- mgr.Run(context.TODO(), taskName(), func(ctx context.Context) error {
				time.Sleep(time.Duration(rand.Intn(10240)+5120) * time.Millisecond)
				return nil
			})
		}()
	}

	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()

	for ntasks > 0 {
		select {
		case <-ticker.C:
			t.Logf("%s", mgr)
		case <-errCh:
			ntasks--
		}
	}
}
