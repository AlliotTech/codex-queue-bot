// Package messaging contains small indirection layers shared by message
// adapters and job notifications.  Keeping the proxy stable means a running
// job never retains a client that a configuration reload has already closed.
package messaging

import (
	"context"
	"errors"
	"sync"
)

var ErrUnavailable = errors.New("messaging client is unavailable")

type Messenger interface {
	Send(ctx context.Context, to, content, traceID string) error
}

type Proxy struct {
	mu     sync.RWMutex
	client Messenger
}

func NewProxy(client Messenger) *Proxy {
	return &Proxy{client: client}
}

func (p *Proxy) Set(client Messenger) {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.client = client
	p.mu.Unlock()
}

func (p *Proxy) Client() Messenger {
	if p == nil {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.client
}

func (p *Proxy) Send(ctx context.Context, to, content, traceID string) error {
	client := p.Client()
	if client == nil {
		return ErrUnavailable
	}
	return client.Send(ctx, to, content, traceID)
}
