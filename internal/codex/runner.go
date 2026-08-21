package codex

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	mathrand "math/rand"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"codex-queue-bot/internal/config"
	"codex-queue-bot/internal/proxyenv"
)

const (
	providerID       = "queue_proxy"
	apiKeyEnvName    = "CODEX_QUEUE_TARGET_API_KEY"
	maxOutputLen     = 64 * 1024
	maxDiagnosticLen = 4096
)

type Runner struct {
	mu              sync.RWMutex
	Binary          string
	PromptsFile     string
	Timeout         time.Duration
	ReasoningEffort string
	Overrides       []string
	Logger          *slog.Logger
}

type runnerSettings struct {
	Binary          string
	PromptsFile     string
	Timeout         time.Duration
	ReasoningEffort string
	Overrides       []string
}

type Result struct {
	Success  bool
	Response string
	Error    string
	Duration time.Duration
}

func (r *Runner) Check() error {
	settings := r.snapshot()
	resolved, err := exec.LookPath(settings.Binary)
	if err != nil {
		return fmt.Errorf("find Codex executable %q: %w", settings.Binary, err)
	}
	if !filepath.IsAbs(resolved) {
		resolved, err = filepath.Abs(resolved)
		if err != nil {
			return fmt.Errorf("resolve Codex executable %q: %w", settings.Binary, err)
		}
	}
	r.mu.Lock()
	r.Binary = resolved
	r.mu.Unlock()
	if _, err := readPrompts(settings.PromptsFile); err != nil {
		return err
	}
	return nil
}

func (r *Runner) Run(ctx context.Context, target config.Target, attempt int) Result {
	started := time.Now()
	result := Result{}
	settings := r.snapshot()

	prompt, err := pickPrompt(settings.PromptsFile)
	if err != nil {
		result.Error = err.Error()
		result.Duration = time.Since(started)
		return result
	}

	workspace, err := os.MkdirTemp("", "codex-queue-work-")
	if err != nil {
		result.Error = fmt.Sprintf("create temporary workspace: %v", err)
		result.Duration = time.Since(started)
		return result
	}
	defer r.removeWorkspace(workspace)

	responseFile, err := os.CreateTemp("", "codex-queue-response-")
	if err != nil {
		result.Error = fmt.Sprintf("create response file: %v", err)
		result.Duration = time.Since(started)
		return result
	}
	responsePath := responseFile.Name()
	_ = responseFile.Close()
	defer os.Remove(responsePath)

	requestID := newRequestID()
	fullPrompt := fmt.Sprintf("%s\n\nAnswer concisely in at most 80 words. Do not inspect local files, run commands, browse, or use tools. Request ID: %s. Attempt: %d", prompt, requestID, attempt)
	args := r.argsWithSettings(settings, target, workspace, responsePath, fullPrompt)

	requestCtx, cancel := context.WithTimeout(ctx, settings.Timeout)
	defer cancel()
	cmd := exec.Command(settings.Binary, args...)
	cmd.Dir = workspace
	cmd.Env = codexEnvironment(os.Environ(), target.APIKey)

	output := newTailBuffer(maxOutputLen)
	cmd.Stdout = output
	cmd.Stderr = output
	runErr := runCommand(requestCtx, cmd)
	diagnostic := cleanDiagnostic(output.String())
	diagnostic = redact(diagnostic, target.APIKey)
	if requestCtx.Err() != nil {
		if errors.Is(requestCtx.Err(), context.DeadlineExceeded) {
			result.Error = fmt.Sprintf("request timed out after %s", settings.Timeout)
		} else {
			result.Error = "request cancelled"
		}
		result.Duration = time.Since(started)
		return result
	}
	if runErr != nil {
		result.Error = fmt.Sprintf("codex exit failed: %v", runErr)
		if diagnostic != "" {
			result.Error += ": " + diagnostic
		}
		result.Duration = time.Since(started)
		return result
	}

	response, err := os.ReadFile(responsePath)
	if err != nil {
		result.Error = fmt.Sprintf("read Codex response: %v", err)
		result.Duration = time.Since(started)
		return result
	}
	result.Response = strings.TrimSpace(string(response))
	if result.Response == "" {
		result.Error = "Codex exited successfully but returned an empty final response"
		if diagnostic != "" {
			result.Error += ": " + diagnostic
		}
		result.Duration = time.Since(started)
		return result
	}

	result.Success = true
	result.Duration = time.Since(started)
	return result
}

func (r *Runner) args(target config.Target, workspace, responsePath, prompt string) []string {
	return r.argsWithSettings(r.snapshot(), target, workspace, responsePath, prompt)
}

func (r *Runner) argsWithSettings(settings runnerSettings, target config.Target, workspace, responsePath, prompt string) []string {
	args := []string{
		"exec",
		"--ignore-user-config",
		"--ephemeral",
		"--skip-git-repo-check",
		"--ignore-rules",
		"--sandbox", "read-only",
		"--color", "never",
		"-C", workspace,
		"-m", target.Model,
		"-c", tomlAssignment("model_provider", providerID),
		"-c", tomlAssignment("model_providers."+providerID+".name", target.Name),
		"-c", tomlAssignment("model_providers."+providerID+".base_url", target.APIBaseURL),
		"-c", tomlAssignment("model_providers."+providerID+".env_key", apiKeyEnvName),
		"-c", tomlAssignment("model_providers."+providerID+".wire_api", target.WireAPI),
		"-c", "model_providers." + providerID + ".requires_openai_auth=false",
		"-c", tomlAssignment("model_reasoning_effort", settings.ReasoningEffort),
		"-c", "project_doc_max_bytes=0",
		"-c", "history.persistence=\"none\"",
		"-c", "features.memories=false",
		"-c", "features.skills=false",
		"-c", "features.multi_agent=false",
		"-c", "features.shell_snapshot=false",
		"-c", "notify=[]",
		"-c", "disable_response_storage=true",
	}
	for _, override := range settings.Overrides {
		args = append(args, "-c", override)
	}
	for _, override := range target.ConfigOverrides {
		args = append(args, "-c", override)
	}
	// Keep this after user-provided overrides so the health-check prompt cannot
	// expose the provider key through a model-spawned command.
	args = append(args, "-c", "shell_environment_policy.inherit=\"none\"")
	args = append(args, "--output-last-message", responsePath, prompt)
	return args
}

// UpdateRuntime changes request-level settings for future Run calls. Each Run
// snapshots all settings at its start, so in-flight requests are unaffected.
func (r *Runner) UpdateRuntime(timeout time.Duration, reasoningEffort string, overrides []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Timeout = timeout
	r.ReasoningEffort = reasoningEffort
	r.Overrides = append([]string(nil), overrides...)
}

func (r *Runner) snapshot() runnerSettings {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return runnerSettings{
		Binary:          r.Binary,
		PromptsFile:     r.PromptsFile,
		Timeout:         r.Timeout,
		ReasoningEffort: r.ReasoningEffort,
		Overrides:       append([]string(nil), r.Overrides...),
	}
}

func (r *Runner) removeWorkspace(path string) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) && r.Logger != nil {
		r.Logger.Warn("temporary Codex workspace was not empty; left in place", "path", path, "error", err)
	}
}

func readPrompts(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open prompts file %q: %w", path, err)
	}
	defer f.Close()

	var prompts []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSuffix(scanner.Text(), "\r")
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		prompts = append(prompts, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read prompts file %q: %w", path, err)
	}
	if len(prompts) == 0 {
		return nil, fmt.Errorf("prompts file %q has no usable prompts", path)
	}
	return prompts, nil
}

func pickPrompt(path string) (string, error) {
	prompts, err := readPrompts(path)
	if err != nil {
		return "", err
	}
	return prompts[mathrand.Intn(len(prompts))], nil
}

func tomlAssignment(key, value string) string {
	return key + "=" + strconv.Quote(value)
}

func newRequestID() string {
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("%d-%s", time.Now().Unix(), hex.EncodeToString(b))
}

func codexEnvironment(base []string, apiKey string) []string {
	base = proxyenv.Normalize(base)
	env := make([]string, 0, len(base)+1)
	for _, item := range base {
		name, _, ok := strings.Cut(item, "=")
		if !ok || name == apiKeyEnvName || !allowedCodexEnvironmentVariable(name) {
			continue
		}
		env = append(env, item)
	}
	return append(env, apiKeyEnvName+"="+apiKey)
}

func allowedCodexEnvironmentVariable(name string) bool {
	if strings.HasPrefix(name, "LC_") {
		return true
	}
	switch name {
	case
		"PATH",
		"HOME",
		"USER",
		"LOGNAME",
		"TMPDIR",
		"TMP",
		"TEMP",
		"LANG",
		"LANGUAGE",
		"TZ",
		"HTTP_PROXY",
		"HTTPS_PROXY",
		"ALL_PROXY",
		"NO_PROXY",
		"http_proxy",
		"https_proxy",
		"all_proxy",
		"no_proxy",
		"SSL_CERT_FILE",
		"SSL_CERT_DIR",
		"REQUESTS_CA_BUNDLE",
		"CURL_CA_BUNDLE",
		"NODE_EXTRA_CA_CERTS":
		return true
	default:
		return false
	}
}

func cleanDiagnostic(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.Join(strings.Fields(raw), " ")
	runes := []rune(raw)
	if len(runes) > maxDiagnosticLen {
		raw = string(runes[len(runes)-maxDiagnosticLen:])
	}
	return raw
}

func redact(value string, secrets ...string) string {
	for _, secret := range secrets {
		if secret != "" {
			value = strings.ReplaceAll(value, secret, "[REDACTED]")
		}
	}
	return value
}

func TargetHost(target config.Target) string {
	u, err := url.Parse(target.APIBaseURL)
	if err != nil {
		return target.APIBaseURL
	}
	return u.Host
}

type tailBuffer struct {
	mu   sync.Mutex
	data []byte
	max  int
}

func newTailBuffer(maximum int) *tailBuffer {
	return &tailBuffer{max: maximum, data: make([]byte, 0, maximum)}
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	written := len(p)
	if b.max <= 0 {
		return written, nil
	}
	if len(p) >= b.max {
		b.data = append(b.data[:0], p[len(p)-b.max:]...)
		return written, nil
	}
	overflow := len(b.data) + len(p) - b.max
	if overflow > 0 {
		copy(b.data, b.data[overflow:])
		b.data = b.data[:len(b.data)-overflow]
	}
	b.data = append(b.data, p...)
	return written, nil
}

func (b *tailBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.data)
}
