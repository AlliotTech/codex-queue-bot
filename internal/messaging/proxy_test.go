package messaging

import (
	"context"
	"testing"
)

type recorder struct{ content string }

func (r *recorder) Send(_ context.Context, _, content, _ string) error {
	r.content = content
	return nil
}

func TestProxyForwardsToCurrentClient(t *testing.T) {
	oldClient := &recorder{}
	newClient := &recorder{}
	proxy := NewProxy(oldClient)
	proxy.Set(newClient)
	if err := proxy.Send(context.Background(), "user", "success", "trace"); err != nil {
		t.Fatal(err)
	}
	if oldClient.content != "" || newClient.content != "success" {
		t.Fatalf("old=%q new=%q", oldClient.content, newClient.content)
	}
}
