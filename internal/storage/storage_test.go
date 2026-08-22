package storage

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"codex-queue-bot/internal/config"
)

func encodedKey(fill byte) string {
	return base64.StdEncoding.EncodeToString([]byte(strings.Repeat(string([]byte{fill}), 32)))
}

func TestFreshDatabaseSetupEncryptionPermissionsAndPersistence(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "data")
	path := filepath.Join(dir, "config.db")
	key := encodedKey('k')
	store, err := Open(ctx, Options{Path: path, MasterKeyBase64: key})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.SetupRequired || len(snapshot.Config.Codex.Targets) != 0 || snapshot.Config.Web.CookieSecure {
		t.Fatalf("fresh snapshot = %+v", snapshot)
	}
	if runtime.GOOS != "windows" {
		if info, err := os.Stat(dir); err != nil || info.Mode().Perm() != 0o700 {
			t.Fatalf("database directory mode = %v, err=%v", info.Mode().Perm(), err)
		}
		if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("database mode = %v, err=%v", info.Mode().Perm(), err)
		}
	}

	snapshot, err = store.SetupAdmin(ctx, "owner", "correct-horse-battery")
	if err != nil || snapshot.SetupRequired {
		t.Fatalf("SetupAdmin: snapshot=%+v err=%v", snapshot, err)
	}
	if username, ok, err := store.VerifyAdmin(ctx, "owner", "correct-horse-battery"); err != nil || !ok || username != "owner" {
		t.Fatalf("VerifyAdmin = %q, %t, %v", username, ok, err)
	}

	target := config.Target{SortOrder: -1, Name: "main", APIBaseURL: "https://api.example/v1", APIKey: "target-plain-secret", Model: "gpt-test", WireAPI: "responses"}
	snapshot, created, err := store.CreateTarget(ctx, target)
	if err != nil || created.ID == 0 || len(snapshot.Config.Codex.Targets) != 1 {
		t.Fatalf("CreateTarget: created=%+v snapshot=%+v err=%v", created, snapshot, err)
	}
	token := "openilink-plain-secret"
	openConfig := snapshot.Config.OpenILink
	openConfig.Enabled = false
	snapshot, err = store.UpdateOpenILink(ctx, openConfig, &token)
	if err != nil || snapshot.Config.OpenILink.Token != token {
		t.Fatalf("UpdateOpenILink: snapshot=%+v err=%v", snapshot, err)
	}
	telegramToken := "telegram-plain-secret"
	telegramConfig := snapshot.Config.Telegram
	telegramConfig.Enabled = false
	snapshot, err = store.UpdateTelegram(ctx, telegramConfig, &telegramToken)
	if err != nil || snapshot.Config.Telegram.Token != telegramToken {
		t.Fatalf("UpdateTelegram: snapshot=%+v err=%v", snapshot, err)
	}

	updated := created
	updated.Name = "renamed"
	updated.Model = "gpt-next"
	snapshot, updated, err = store.UpdateTarget(ctx, created.ID, updated, nil)
	if err != nil || updated.APIKey != "target-plain-secret" {
		t.Fatalf("UpdateTarget keep key: target=%+v err=%v", updated, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"target-plain-secret", "openilink-plain-secret", "telegram-plain-secret", "correct-horse-battery"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("database contains plaintext %q", forbidden)
		}
	}

	reopened, err := Open(ctx, Options{Path: path, MasterKeyBase64: key})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	persisted, err := reopened.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := persisted.Config.Codex.Targets[0]; got.Name != "renamed" || got.Model != "gpt-next" || got.APIKey != "target-plain-secret" {
		t.Fatalf("persisted target = %+v", got)
	}
	if persisted.Config.OpenILink.Token != token {
		t.Fatalf("persisted token = %q", persisted.Config.OpenILink.Token)
	}
	if persisted.Config.Telegram.Token != telegramToken {
		t.Fatalf("persisted Telegram token = %q", persisted.Config.Telegram.Token)
	}
}

func TestWrongMasterKeyFailsBeforeLoadingConfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.db")
	store, err := Open(context.Background(), Options{Path: path, MasterKeyBase64: encodedKey('a')})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = Open(context.Background(), Options{Path: path, MasterKeyBase64: encodedKey('b')})
	if !errors.Is(err, ErrWrongMasterKey) {
		t.Fatalf("wrong-key error = %v", err)
	}
}

func TestPromptListFallsBackToFileThenPersistsInExistingCodexJSON(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	promptFile := filepath.Join(dir, "prompts.txt")
	if err := os.WriteFile(promptFile, []byte("first prompt\n# comment\nsecond prompt\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := Open(ctx, Options{Path: filepath.Join(dir, "config.db"), MasterKeyBase64: encodedKey('p')})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	snapshot, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate an older codex JSON record with no prompts property.
	if _, err := store.db.ExecContext(ctx, `UPDATE config_sections SET data = json_set(json_remove(data, '$.prompts'), '$.prompts_file', ?) WHERE name = 'codex'`, promptFile); err != nil {
		t.Fatal(err)
	}
	snapshot, err = store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(snapshot.Config.Codex.Prompts, "|"); got != "first prompt|second prompt" {
		t.Fatalf("fallback prompts = %q", got)
	}
	snapshot.Config.Codex.Prompts = []string{"database prompt"}
	if _, err := store.UpdateCodex(ctx, snapshot.Config.Codex); err != nil {
		t.Fatal(err)
	}
	var rawCodex string
	if err := store.db.QueryRowContext(ctx, `SELECT data FROM config_sections WHERE name = 'codex'`).Scan(&rawCodex); err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(rawCodex), &fields); err != nil {
		t.Fatal(err)
	}
	var storedPrompts []string
	if raw, ok := fields["prompts"]; !ok {
		t.Fatal("saved codex JSON is missing prompts property")
	} else if err := json.Unmarshal(raw, &storedPrompts); err != nil {
		t.Fatalf("decode saved prompts: %v", err)
	}
	if got := strings.Join(storedPrompts, "|"); got != "database prompt" {
		t.Fatalf("saved prompts property = %q", got)
	}
	if err := os.WriteFile(promptFile, []byte("changed file prompt\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	persisted, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(persisted.Config.Codex.Prompts, "|"); got != "database prompt" {
		t.Fatalf("persisted prompts = %q", got)
	}

	persisted.Config.Codex.Prompts = []string{}
	if _, err := store.UpdateCodex(ctx, persisted.Config.Codex); err == nil || !strings.Contains(err.Error(), "at least one prompt") {
		t.Fatalf("empty prompt list error = %v", err)
	}
	unchanged, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(unchanged.Config.Codex.Prompts, "|"); got != "database prompt" {
		t.Fatalf("rejected empty save changed prompts = %q", got)
	}
}

func TestSchemaUpgradeAppliesMissingMigrations(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "upgrade.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, migrations[0].sql); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO schema_migrations(version) VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := Open(ctx, Options{Path: path, MasterKeyBase64: encodedKey('u')})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var count int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations`).Scan(&count); err != nil || count != len(migrations) {
		t.Fatalf("migration count = %d, err=%v", count, err)
	}
}

func TestSchemaUpgradeAddsTelegramSectionToInitializedDatabase(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "upgrade-telegram.db")
	key := encodedKey('t')
	store, err := Open(ctx, Options{Path: path, MasterKeyBase64: key})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `DELETE FROM config_sections WHERE name = 'telegram'`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `DELETE FROM schema_migrations WHERE version = 3`); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, Options{Path: path, MasterKeyBase64: key})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	snapshot, err := reopened.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Config.Telegram.Enabled || snapshot.Config.Telegram.BaseURL != "https://api.telegram.org" || snapshot.Config.Telegram.HTTPTimeoutSecond != 45 || snapshot.Config.Telegram.PollTimeoutSecond != 30 {
		t.Fatalf("migrated Telegram config = %+v", snapshot.Config.Telegram)
	}
}

func TestLegacyConfigPathIsIgnored(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	legacyPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(filepath.Join(dir, "legacy-prompts.txt"), []byte("hello\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("LEGACY_TOKEN", "legacy-token-secret")
	t.Setenv("LEGACY_KEY", "legacy-target-secret")
	legacy := `{
  "openilink": {"enabled": true, "base_url": "https://hub.example", "token_env": "LEGACY_TOKEN"},
  "web": {"admin_username": "legacy-owner", "cookie_secure": false},
  "codex": {"prompts_file": "legacy-prompts.txt", "targets": [
    {"name":"main","api_base_url":"https://api.example/v1","api_key_env":"LEGACY_KEY","model":"gpt-test"}
  ]}
}`
	if err := os.WriteFile(legacyPath, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "data", "config.db")
	key := encodedKey('m')
	store, err := Open(ctx, Options{Path: path, MasterKeyBase64: key, LegacyConfigPath: legacyPath})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.SuggestedUsername != "admin" || snapshot.Config.OpenILink.Token != "" || len(snapshot.Config.Codex.Targets) != 0 {
		t.Fatalf("legacy config was unexpectedly imported = %+v", snapshot)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(legacyPath, []byte(`{"broken":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, Options{Path: path, MasterKeyBase64: key, LegacyConfigPath: legacyPath})
	if err != nil {
		t.Fatalf("initialized database reread legacy JSON: %v", err)
	}
	defer reopened.Close()
}

func TestIgnoredLegacySecretsNeverEnterDatabase(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	legacyPath := filepath.Join(dir, "config.json")
	t.Setenv("DISABLED_LEGACY_TOKEN", "disabled-token-secret")
	legacy := `{
  "openilink": {"enabled": false, "base_url": "https://hub.example", "token_env": "DISABLED_LEGACY_TOKEN"},
  "codex": {"targets": [
    {"name":"direct","api_base_url":"https://api.example/v1","api_key":"direct-target-secret","model":"gpt-test"}
  ]}
}`
	if err := os.WriteFile(legacyPath, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "data", "config.db")
	store, err := Open(ctx, Options{Path: path, MasterKeyBase64: encodedKey('d'), LegacyConfigPath: legacyPath})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	snapshot, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Config.OpenILinkEnabled() || snapshot.Config.OpenILink.Token != "" || len(snapshot.Config.Codex.Targets) != 0 {
		t.Fatalf("ignored legacy snapshot = %+v", snapshot)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"disabled-token-secret", "direct-target-secret"} {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("legacy import stored plaintext %q", secret)
		}
	}
}

func TestInvalidLegacyConfigDoesNotBlockFreshDatabase(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	legacyPath := filepath.Join(dir, "config.json")
	legacy := `{"openilink":{"enabled":false},"codex":{"targets":[{"name":"atomic-target","api_base_url":"https://api.example/v1","api_key_env":"MISSING_IMPORT_KEY","model":"gpt-test"}]}}`
	if err := os.WriteFile(legacyPath, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "data", "config.db")
	store, err := Open(ctx, Options{Path: path, MasterKeyBase64: encodedKey('r'), LegacyConfigPath: legacyPath})
	if err != nil {
		t.Fatalf("fresh database was blocked by legacy config: %v", err)
	}
	defer store.Close()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "atomic-target") {
		t.Fatal("failed import left partial target data")
	}
	snapshot, _ := store.Load(ctx)
	if len(snapshot.Config.Codex.Targets) != 0 {
		t.Fatalf("legacy target was imported = %+v", snapshot)
	}
}

func TestSetupRaceConcurrentWritesAndRollback(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, Options{Path: filepath.Join(t.TempDir(), "config.db"), MasterKeyBase64: encodedKey('c')})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var successes atomic.Int32
	var wg sync.WaitGroup
	for index := 0; index < 4; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			_, err := store.SetupAdmin(ctx, fmt.Sprintf("owner-%d", index), "correct-horse-battery")
			if err == nil {
				successes.Add(1)
				return
			}
			if !errors.Is(err, ErrAlreadySetup) {
				t.Errorf("SetupAdmin race error = %v", err)
			}
		}(index)
	}
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatalf("setup successes = %d", successes.Load())
	}

	base, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 8; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			web := base.Config.Web
			web.ActivityLimit = 100 + index
			if _, err := store.UpdateWeb(ctx, web); err != nil {
				t.Errorf("concurrent UpdateWeb: %v", err)
			}
		}(index)
	}
	wg.Wait()
	afterWrites, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if afterWrites.Revision != base.Revision+8 {
		t.Fatalf("revision after concurrent writes = %d, want %d", afterWrites.Revision, base.Revision+8)
	}

	target := config.Target{SortOrder: 0, Name: "duplicate", APIBaseURL: "https://api.example/v1", APIKey: "secret", Model: "m", WireAPI: "responses"}
	beforeCreate := afterWrites.Revision
	if _, _, err := store.CreateTarget(ctx, target); err != nil {
		t.Fatal(err)
	}
	afterCreate, _ := store.Load(ctx)
	target.Name = "DUPLICATE"
	if _, _, err := store.CreateTarget(ctx, target); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate create error = %v", err)
	}
	afterRollback, _ := store.Load(ctx)
	if afterRollback.Revision != beforeCreate+1 || afterRollback.Revision != afterCreate.Revision || len(afterRollback.Config.Codex.Targets) != 1 {
		t.Fatalf("rollback snapshot = %+v", afterRollback)
	}

	var migrationCount int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations`).Scan(&migrationCount); err != nil || migrationCount != len(migrations) {
		t.Fatalf("migration count = %d, err=%v", migrationCount, err)
	}
}
