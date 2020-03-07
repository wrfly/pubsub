package pubsub

import (
	"context"
)

type PubSuber interface {
	PubSub(ctx context.Context, ch string) (SendChan, RcvChan, error)
	Pub(ctx context.Context, ch string) (SendChan, error)
	Sub(ctx context.Context, ch string) (RcvChan, error)
	CloseWait(context.Context) error
	Stats() (chanNum, pubNum, subNum int32)
}

type SendChan interface {
	Send([]byte) error
}

type RcvChan interface {
	Receive() <-chan []byte
}
