//go:build !(aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris)

package codex

import (
	"os"
	"os/exec"
)

func configureProcess(*exec.Cmd) {}

func terminateProcess(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return os.ErrProcessDone
	}
	return cmd.Process.Kill()
}

func killProcess(cmd *exec.Cmd) error {
	return terminateProcess(cmd)
}
