package peer

import "context"

type peerKey struct{}

// NewContext creates a new context with peer information attached.
func NewContext(ctx context.Context, address string) context.Context {
	return context.WithValue(ctx, peerKey{}, address)
}

// FromContext returns the peer information in ctx if it exists.
func FromContext(ctx context.Context) (address string, ok bool) {
	address, ok = ctx.Value(peerKey{}).(string)
	return
}
