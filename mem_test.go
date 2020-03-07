package pubsub

// go test -timeout 30s github.com/wrfly/chatom/pkg/pubsub -v -count=1 -race

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func generateMsg(ctx context.Context, ch PubChan, g *int32) {
	tk := time.NewTicker(time.Second / 200)
	defer tk.Stop()
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			s := fmt.Sprintf("index: %d", i)
			if err := ch.Write([]byte(s)); err == nil {
				atomic.AddInt32(g, 1)
			}
		}
	}

}

func TestOneToOne(t *testing.T) {
	pubsub := NewMemPubSuber()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pubChan, err := pubsub.Pub(ctx, "1")
	if err != nil {
		t.Fatal(err)
	}
	var sent int32
	go generateMsg(ctx, pubChan, &sent)

	subChan, err := pubsub.Sub(ctx, "1")
	if err != nil {
		t.Fatal(err)
	}
	var rcv int32
	for x := range subChan.Read() {
		if len(x) != 0 {
			rcv++
		}
	}
	// after 1s
	<-ctx.Done()

	ctx, _ = context.WithTimeout(context.Background(), time.Second)
	if err := pubsub.CloseWait(ctx); err != nil {
		t.Error(err)
	}

	if sent != rcv {
		t.Errorf("sent: %d, rcv: %d", sent, rcv)
	}

	if c, p, s := pubsub.Stats(); c != 0 || p != 0 || s != 0 {
		t.Errorf("??? c:%d, p:%d, s:%d", c, p, s)
	}

}

func TestOnToMulti(t *testing.T) {
	pubsub := NewMemPubSuber()

	if c, p, s := pubsub.Stats(); c != 0 || p != 0 || s != 0 {
		t.Fatalf("??? c:%d, p:%d, s:%d", c, p, s)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pubChan, err := pubsub.Pub(ctx, "1")
	if err != nil {
		t.Fatal(err)
	}

	subGot := make([]int, 10)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			subChan, err := pubsub.Sub(ctx, "1")
			if err != nil {
				t.Fatal(err)
			}
			for x := range subChan.Read() {
				if len(x) != 0 {
					subGot[i]++
				}
			}
			wg.Done()
		}(i)
	}

	var sent int32
	go func() {
		wg.Add(1)
		generateMsg(ctx, pubChan, &sent)
		wg.Done()
	}()

	<-ctx.Done()

	ctx, _ = context.WithTimeout(context.Background(), time.Second)
	if err := pubsub.CloseWait(ctx); err != nil {
		t.Error(err)
	}

	wg.Wait()
	for i, g := range subGot {
		if g < int(sent) {
			t.Errorf("sub [%d] got %d (sent %d)", i, g, sent)
		}
	}

}

func TestMultiToMulti(t *testing.T) {
	pubsub := NewMemPubSuber()

	if c, p, s := pubsub.Stats(); c != 0 || p != 0 || s != 0 {
		t.Fatalf("??? c:%d, p:%d, s:%d", c, p, s)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// create a channel first
	pubsub.Pub(ctx, "1")

	const num = 10
	var (
		wg    sync.WaitGroup
		rcvWG sync.WaitGroup
	)

	subGot := make([]int, num)
	for i := 0; i < num; i++ {
		wg.Add(1)
		rcvWG.Add(1)
		go func(i int) {
			subChan, err := pubsub.Sub(ctx, "1")
			if err != nil {
				t.Fatal(err)
			}
			rcvWG.Done()
			for x := range subChan.Read() {
				if len(x) != 0 {
					subGot[i]++
				}
			}
			wg.Done()
		}(i)
	}
	rcvWG.Wait()

	var sent int32
	for i := 0; i < num; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pubChan, err := pubsub.Pub(ctx, "1")
			if err != nil {
				t.Fatal(err)
			}
			// send N messages
			for i := 0; i < num; i++ {
				if err := pubChan.Write([]byte("x")); err != nil {
					t.Error(err)
				} else {
					atomic.AddInt32(&sent, 1)
				}
			}
		}(i)
	}

	<-ctx.Done()

	ctx, _ = context.WithTimeout(context.Background(), time.Second)
	if err := pubsub.CloseWait(ctx); err != nil {
		t.Error(err)
	}

	wg.Wait()
	for i, g := range subGot {
		if g < int(sent) {
			t.Errorf("sub [%d] got %d, sent %d", i, g, sent)
		}
	}
}
