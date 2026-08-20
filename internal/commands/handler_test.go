package commands

import (
	"context"
	"reflect"
	"testing"
	"time"

	"codex-queue-bot/internal/config"
	"codex-queue-bot/internal/jobs"
)

func TestParseCommandAndTargets(t *testing.T) {
	command, args, ok := parseCommand("  /开挤@bot  a,b，c  ")
	if !ok || command != "开挤" || args != "a,b，c" {
		t.Fatalf("parseCommand = %q %q %v", command, args, ok)
	}
	want := []string{"a", "b", "c"}
	if got := splitTargets(args); !reflect.DeepEqual(got, want) {
		t.Fatalf("splitTargets = %#v, want %#v", got, want)
	}
}

func TestFormatSnapshotSeparatesFailureDetails(t *testing.T) {
	snapshot := jobs.Snapshot{
		Name:      "backup",
		State:     jobs.StateRunning,
		Attempts:  53,
		LastError: "codex exit failed: exit status 1",
	}

	want := "backup：挤队中，第 53 次\n\n最近失败：codex exit failed: exit status 1"
	if got := formatSnapshot(snapshot, time.Now()); got != want {
		t.Fatalf("formatSnapshot = %q, want %q", got, want)
	}
}

func TestStatusSeparatesTargetsWithBlankLine(t *testing.T) {
	manager := jobs.New(
		context.Background(),
		[]config.Target{{Name: "primary"}, {Name: "backup"}},
		nil,
		nil,
		nil,
		time.Second,
		time.Second,
		1,
		"开蹬",
	)
	handler := New(manager, nil, nil, nil)

	want := "primary：未启动\n\nbackup：未启动"
	if got := handler.status(nil); got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
}
