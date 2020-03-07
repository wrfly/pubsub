package pubsub

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// global channels
var globalChans = make(map[string]*channel, 100)

// errors
var (
	ErrChNotFound  = fmt.Errorf("channel not found")
	ErrChanClosed  = fmt.Errorf("channel closed")
	ErrPubExceeded = fmt.Errorf("puber exceeded")
	ErrSubExceeded = fmt.Errorf("suber exceeded")
	ErrWaitTimeout = fmt.Errorf("wait timeout")
	ErrSendTimeout = fmt.Errorf("send timeout")
)

type memChan struct {
	chName string

	closed *int32
	ch     chan []byte
	m      sync.Mutex

	tout time.Duration // send timeout
	fail chan uint16
}

func (c *memChan) Write(bs []byte) error {
	if atomic.LoadInt32(c.closed) == 0 {
		if err := c.subSend(bs); err != nil {
			return err
		}

		if f := <-c.fail; f != 0 {
			return fmt.Errorf("faild %d", f)
		}
		return nil
	}
	return ErrChanClosed
}

func (c *memChan) subSend(bs []byte) error {
	send := make([]byte, len(bs))
	copy(send, bs)
	c.m.Lock()
	defer c.m.Unlock()
	if atomic.LoadInt32(c.closed) == 0 {
		tmer := time.NewTimer(c.tout)
		defer tmer.Stop()
		select {
		case <-tmer.C:
			return ErrSendTimeout
		case c.ch <- send:
			return nil
		}
	}
	return ErrChanClosed
}

func (c *memChan) Read() <-chan []byte {
	return c.ch
}

func (c *memChan) Close() {
	if atomic.CompareAndSwapInt32(c.closed, 0, 1) {
		c.m.Lock()
		close(c.ch)
		c.m.Unlock()
	}
}

type channel struct {
	pubChan  *memChan
	subChans []*memChan
	name     string

	ctx    context.Context
	cancel context.CancelFunc

	boardCast  context.Context
	boardCastC context.CancelFunc

	m sync.Mutex

	pubWG sync.WaitGroup
	subWG sync.WaitGroup
	pubs  *int32
	subs  *int32
}

func NewMemPubSuber() PubSuber {
	var pn, sn int32
	return &pubSuber{
		chans:  make(map[string]*channel, 100),
		pubs:   &pn,
		subs:   &sn,
		maxPub: 100, // TODO: make it configurable
		maxSub: 100, // TODO: make it configurable
	}
}

type pubSuber struct {
	chans map[string]*channel
	m     sync.RWMutex

	// stats
	pubs   *int32 // how many pubs in total
	subs   *int32 // how many subs in total
	maxPub int32  // max pub of each channel
	maxSub int32  // max sub of each channel
}

func (ps *pubSuber) Pub(ctx context.Context, chName string) (PubChan, error) {
	ps.m.Lock()
	defer ps.m.Unlock()

	ch, exist := ps.chans[chName]
	if exist {
		if err := ps.addPub(ctx, ch); err != nil {
			return nil, err
		}
		return ch.pubChan, nil
	}

	// not exist, create new one
	var pn, sn int32
	chCtx, cancel := context.WithCancel(context.Background())
	var closed int32
	ch = &channel{
		pubChan: &memChan{
			chName: "pub:" + chName,
			closed: &closed,
			ch:     make(chan []byte),
			tout:   time.Millisecond * 10,
			fail:   make(chan uint16),
		},
		subChans: make([]*memChan, 0),
		name:     chName,
		pubs:     &pn,
		subs:     &sn,
		ctx:      chCtx,
		cancel:   cancel,
	}
	ps.chans[chName] = ch

	if err := ps.addPub(ctx, ch); err != nil {
		return nil, err
	}

	// boardcast
	go func() {
		ch.boardCast, ch.boardCastC = context.WithCancel(context.Background())
		for bs := range ch.pubChan.ch {
			send := make([]byte, len(bs))
			copy(send, bs)
			ch.m.Lock()
			failedSubs := uint16(len(ch.subChans))
			for i := range ch.subChans {
				if err := ch.subChans[i].subSend(send); err == nil {
					failedSubs--
				}
			}
			ch.m.Unlock()
			ch.pubChan.fail <- failedSubs
		}
		ch.boardCastC()
	}()

	// no publisher and close this channel
	go func() {
		ch.pubWG.Wait()    // no puber
		ch.pubChan.Close() // close pub chan
		ch.cancel()        // notify all subers
		ch.subWG.Wait()    // wait subers done

		<-ch.boardCast.Done()  // wait for board cast done
		close(ch.pubChan.fail) // and close the fail chan

		// delete channel from ps
		ps.m.Lock()
		delete(ps.chans, chName)
		ps.m.Unlock()
	}()

	return ch.pubChan, nil
}

func (ps *pubSuber) Sub(ctx context.Context, chName string) (SubChan, error) {
	ch, exist := ps.chans[chName]
	if !exist {
		return nil, ErrChNotFound
	}

	var closed int32
	subChan := &memChan{
		chName: "sub:" + chName,
		closed: &closed,
		ch:     make(chan []byte, 10),
		tout:   time.Millisecond * 10, // TODO: make it configurable
	}

	if err := ps.addSub(ctx, ch, subChan); err != nil {
		return nil, err
	}

	return subChan, nil
}

func (ps *pubSuber) CloseWait(ctx context.Context) error {
	closed := make(chan bool)
	go func() {
		for ctx.Err() == nil {
			cNum, _, _ := ps.Stats()
			if cNum == 0 {
				break
			}
			time.Sleep(time.Millisecond * 10)
		}
		close(closed)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-closed:
		return nil
	}
}

func (ps *pubSuber) Stats() (chanNum, pubNum, subNum int32) {
	ps.m.RLock()
	l := len(ps.chans)
	ps.m.RUnlock()
	return int32(l), atomic.LoadInt32(ps.pubs), atomic.LoadInt32(ps.subs)
}

type pubSubChan struct {
	p PubChan
	s SubChan
}

func (ps *pubSubChan) Write(bs []byte) error {
	return ps.p.Write(bs)
}

func (ps *pubSubChan) Read() <-chan []byte {
	return ps.s.Read()
}

func (ps *pubSuber) PubSub(ctx context.Context, chName string) (PubSubChan, error) {
	pubCh, err := ps.Pub(ctx, chName)
	if err != nil {
		return nil, err
	}
	subCh, err := ps.Sub(ctx, chName)

	return &pubSubChan{pubCh, subCh}, err
}

func (ps *pubSuber) addPub(ctx context.Context, ch *channel) error {
	if atomic.AddInt32(ch.pubs, 1) > ps.maxPub {
		return ErrPubExceeded
	}

	atomic.AddInt32(ps.pubs, 1)

	ch.pubWG.Add(1)
	go func() {
		<-ctx.Done() // no longer pub
		ch.pubWG.Done()

		atomic.AddInt32(ch.pubs, -1)
		atomic.AddInt32(ps.pubs, -1)
	}()

	return nil
}

func (ps *pubSuber) addSub(ctx context.Context, ch *channel, subChan *memChan) error {

	if atomic.AddInt32(ch.subs, 1) > ps.maxSub {
		return ErrSubExceeded
	}

	ch.m.Lock()
	ch.subChans = append(ch.subChans, subChan)
	ch.m.Unlock()

	atomic.AddInt32(ps.subs, 1)

	// add sub
	ch.subWG.Add(1)
	go func() {
		select {
		case <-ctx.Done(): // no longer sub
		case <-ch.ctx.Done(): // no longer pub
		}
		ch.subWG.Done()
		atomic.AddInt32(ch.subs, -1)
		atomic.AddInt32(ps.subs, -1)

		subChan.Close()
	}()

	return nil
}
