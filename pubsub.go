package pubsub

import (
	"context"
)

// PubSuber can publish or subscrib to a channel
type PubSuber interface {
	PubSub(ctx context.Context, ch string) (PubSubChan, error)
	Pub(ctx context.Context, ch string) (PubChan, error)
	Sub(ctx context.Context, ch string) (SubChan, error)
	CloseWait(context.Context) error
	Stats() (chanNum, pubNum, subNum int32)
}

type PubChan interface {
	Write([]byte) error
}

type SubChan interface {
	Read() <-chan []byte
}

type PubSubChan interface {
	PubChan
	SubChan
}
