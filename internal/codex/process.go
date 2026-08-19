package codex

import (
	"context"
	"os/exec"
	"time"
)

const processShutdownGrace = 5 * time.Second

func runCommand(ctx context.Context, cmd *exec.Cmd) error {
	configureProcess(cmd)
	if err := cmd.Start(); err != nil {
		return err
	}

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		_ = terminateProcess(cmd)
	}

	timer := time.NewTimer(processShutdownGrace)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		_ = killProcess(cmd)
		return <-done
	}
}
