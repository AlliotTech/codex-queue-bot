package storage

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"codex-queue-bot/internal/config"

	"golang.org/x/crypto/argon2"
	_ "modernc.org/sqlite"
)

const (
	metaInitialized                = "app_initialized"
	metaConfigRevision             = "config_revision"
	metaLoadedStartupRevision      = "loaded_startup_revision"
	metaSuggestedUsername          = "suggested_username"
	metaMasterKeyCheck             = "master_key_check"
	masterKeyCheckPlaintext        = "codex-queue-bot-master-key-check-v1"
	masterKeyCheckAAD              = "meta:master_key_check"
	cipherVersion             byte = 1
)

var (
	ErrWrongMasterKey = errors.New("CODEX_QUEUE_MASTER_KEY does not match this database")
	ErrAlreadySetup   = errors.New("administrator setup has already been completed")
	ErrNotFound       = errors.New("record not found")
	ErrConflict       = errors.New("configuration conflicts with an existing record")
)

type Options struct {
	Path             string
	MasterKeyBase64  string
	LegacyConfigPath string
}

type Store struct {
	db   *sql.DB
	key  []byte
	path string
}

type Snapshot struct {
	Config                config.Config
	Revision              int64
	SectionRevisions      map[string]int64
	LoadedStartupRevision int64
	SetupRequired         bool
	SuggestedUsername     string
}

type persistedCodex struct {
	Binary               string   `json:"binary"`
	PromptsFile          string   `json:"prompts_file"`
	RequestTimeoutSecond int      `json:"request_timeout_seconds"`
	RetryMinSecond       int      `json:"retry_min_seconds"`
	RetryMaxSecond       int      `json:"retry_max_seconds"`
	KeepaliveMinSecond   int      `json:"keepalive_min_seconds"`
	KeepaliveMaxSecond   int      `json:"keepalive_max_seconds"`
	MaxParallel          int      `json:"max_parallel"`
	SuccessMessage       string   `json:"success_message"`
	ReasoningEffort      string   `json:"reasoning_effort"`
	ConfigOverrides      []string `json:"config_overrides"`
}

type persistedOpenILink struct {
	Enabled           bool     `json:"enabled"`
	BaseURL           string   `json:"base_url"`
	AllowedUserIDs    []string `json:"allowed_user_ids"`
	HTTPTimeoutSecond int      `json:"http_timeout_seconds"`
}

type persistedTelegram struct {
	Enabled           bool     `json:"enabled"`
	BaseURL           string   `json:"base_url"`
	AllowedUserIDs    []string `json:"allowed_user_ids"`
	HTTPTimeoutSecond int      `json:"http_timeout_seconds"`
	PollTimeoutSecond int      `json:"poll_timeout_seconds"`
}

type persistedWeb struct {
	ListenAddress  string   `json:"listen_address"`
	CookieSecure   bool     `json:"cookie_secure"`
	TrustedProxies []string `json:"trusted_proxies"`
	ActivityLimit  int      `json:"activity_limit"`
}

type migration struct {
	version int
	sql     string
}

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

var migrations = []migration{
	{version: 1, sql: `
CREATE TABLE app_meta (
  key TEXT PRIMARY KEY,
  value BLOB NOT NULL
);
CREATE TABLE administrators (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  username TEXT NOT NULL COLLATE NOCASE UNIQUE,
  password_hash TEXT NOT NULL
);
CREATE TABLE config_sections (
  name TEXT PRIMARY KEY,
  revision INTEGER NOT NULL,
  data TEXT NOT NULL,
  secret BLOB
);
CREATE TABLE targets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sort_order INTEGER NOT NULL,
  revision INTEGER NOT NULL,
  name TEXT NOT NULL COLLATE NOCASE UNIQUE,
  api_base_url TEXT NOT NULL,
  api_key BLOB NOT NULL,
  model TEXT NOT NULL,
  wire_api TEXT NOT NULL,
  config_overrides TEXT NOT NULL
);`},
	{version: 2, sql: `CREATE INDEX targets_sort_order_idx ON targets(sort_order, id);`},
	{version: 3, sql: `
INSERT INTO config_sections(name, revision, data, secret)
SELECT 'telegram', 1, '{"enabled":false,"base_url":"https://api.telegram.org","allowed_user_ids":[],"http_timeout_seconds":45,"poll_timeout_seconds":30}', NULL
WHERE EXISTS (SELECT 1 FROM app_meta WHERE key = 'app_initialized')
  AND NOT EXISTS (SELECT 1 FROM config_sections WHERE name = 'telegram');`},
}

func Open(ctx context.Context, options Options) (*Store, error) {
	key, err := DecodeMasterKey(options.MasterKeyBase64)
	if err != nil {
		return nil, err
	}
	path := strings.TrimSpace(options.Path)
	if path == "" {
		return nil, errors.New("database path is required")
	}
	if path != ":memory:" {
		dir := filepath.Dir(path)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("create database directory: %w", err)
		}
		if !protectedDirectory(dir) {
			if err := os.Chmod(dir, 0o700); err != nil {
				return nil, fmt.Errorf("set database directory permissions: %w", err)
			}
		}
		file, fileErr := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
		if fileErr != nil {
			return nil, fmt.Errorf("create database file: %w", fileErr)
		}
		if fileErr := file.Chmod(0o600); fileErr != nil {
			_ = file.Close()
			return nil, fmt.Errorf("set database permissions: %w", fileErr)
		}
		if fileErr := file.Close(); fileErr != nil {
			return nil, fmt.Errorf("close database file: %w", fileErr)
		}
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open SQLite database: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store := &Store{db: db, key: append([]byte(nil), key...), path: path}
	cleanup := func(openErr error) (*Store, error) {
		_ = db.Close()
		return nil, openErr
	}
	for _, statement := range []string{
		"PRAGMA foreign_keys = ON",
		"PRAGMA busy_timeout = 5000",
		"PRAGMA journal_mode = DELETE",
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return cleanup(fmt.Errorf("configure SQLite: %w", err))
		}
	}
	if err := store.migrate(ctx); err != nil {
		return cleanup(err)
	}
	initialized, err := store.metaBool(ctx, metaInitialized)
	if err != nil {
		return cleanup(err)
	}
	if initialized {
		if err := store.ensureMasterKey(ctx); err != nil {
			return cleanup(err)
		}
	} else {
		// A previous failed startup may have written a verifier with an older
		// version of the application. Verify it when present, but do not create
		// a new one until the initial configuration transaction commits.
		if _, err := store.verifyMasterKey(ctx); err != nil {
			return cleanup(err)
		}
		cfg := config.DefaultDatabaseConfig()
		suggestedUsername := cfg.Web.AdminUsername
		legacyPath := strings.TrimSpace(options.LegacyConfigPath)
		if legacyPath != "" {
			info, statErr := os.Stat(legacyPath)
			switch {
			case statErr == nil && !info.IsDir():
				legacy, loadErr := config.Load(legacyPath)
				if loadErr != nil {
					return cleanup(fmt.Errorf("import legacy configuration: %w", loadErr))
				}
				// Explicitly disabled legacy OpenILink configurations did not need
				// their token at runtime, but the one-time migration must still
				// resolve token_env before discarding the environment-variable name.
				if legacy.OpenILink.Token == "" && strings.TrimSpace(legacy.OpenILink.TokenEnv) != "" {
					envName := strings.TrimSpace(legacy.OpenILink.TokenEnv)
					legacy.OpenILink.Token = strings.TrimSpace(os.Getenv(envName))
					if legacy.OpenILink.Token == "" {
						return cleanup(fmt.Errorf("import legacy configuration: openilink.token_env %q is not set", envName))
					}
				}
				if legacy.Telegram.Token == "" && strings.TrimSpace(legacy.Telegram.TokenEnv) != "" {
					envName := strings.TrimSpace(legacy.Telegram.TokenEnv)
					legacy.Telegram.Token = strings.TrimSpace(os.Getenv(envName))
					if legacy.Telegram.Token == "" {
						return cleanup(fmt.Errorf("import legacy configuration: telegram.token_env %q is not set", envName))
					}
				}
				cfg = *legacy
				suggestedUsername = strings.TrimSpace(cfg.Web.AdminUsername)
			case statErr == nil:
				return cleanup(fmt.Errorf("legacy configuration path %q is not a file", legacyPath))
			case !errors.Is(statErr, os.ErrNotExist):
				return cleanup(fmt.Errorf("inspect legacy configuration: %w", statErr))
			}
		}
		if suggestedUsername == "" {
			suggestedUsername = "admin"
		}
		if err := store.initialize(ctx, cfg, suggestedUsername); err != nil {
			return cleanup(err)
		}
	}
	if path != ":memory:" {
		if err := os.Chmod(path, 0o600); err != nil {
			return cleanup(fmt.Errorf("set database permissions: %w", err))
		}
	}
	if _, err := store.Load(ctx); err != nil {
		return cleanup(err)
	}
	return store, nil
}

func DecodeMasterKey(raw string) ([]byte, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, errors.New("CODEX_QUEUE_MASTER_KEY is required and must be Base64-encoded 32 bytes")
	}
	key, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		key, err = base64.RawStdEncoding.DecodeString(raw)
		if err != nil {
			return nil, fmt.Errorf("decode CODEX_QUEUE_MASTER_KEY: %w", err)
		}
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("CODEX_QUEUE_MASTER_KEY must decode to exactly 32 bytes (got %d)", len(key))
	}
	return key, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY)`); err != nil {
		return fmt.Errorf("create migration table: %w", err)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	applied := map[int]bool{}
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan migration: %w", err)
		}
		applied[version] = true
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close migration rows: %w", err)
	}
	for _, item := range migrations {
		if applied[item.version] {
			continue
		}
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin migration %d: %w", item.version, err)
		}
		if _, err := tx.ExecContext(ctx, item.sql); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("apply migration %d: %w", item.version, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations(version) VALUES (?)`, item.version); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("record migration %d: %w", item.version, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %d: %w", item.version, err)
		}
	}
	return nil
}

func (s *Store) ensureMasterKey(ctx context.Context) error {
	present, err := s.verifyMasterKey(ctx)
	if err != nil || present {
		return err
	}
	value, err := s.encrypt([]byte(masterKeyCheckPlaintext), masterKeyCheckAAD)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO app_meta(key, value) VALUES (?, ?)`, metaMasterKeyCheck, value)
	if err != nil {
		return fmt.Errorf("store master-key check: %w", err)
	}
	return nil
}

func (s *Store) verifyMasterKey(ctx context.Context) (bool, error) {
	var encrypted []byte
	err := s.db.QueryRowContext(ctx, `SELECT value FROM app_meta WHERE key = ?`, metaMasterKeyCheck).Scan(&encrypted)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read master-key check: %w", err)
	}
	plain, err := s.decrypt(encrypted, masterKeyCheckAAD)
	if err != nil || subtle.ConstantTimeCompare(plain, []byte(masterKeyCheckPlaintext)) != 1 {
		return true, ErrWrongMasterKey
	}
	return true, nil
}

func (s *Store) initialize(ctx context.Context, cfg config.Config, suggestedUsername string) error {
	cfg.OpenILink.SetEnabledExplicit(cfg.OpenILinkEnabled())
	cfg.OpenILink.Token = strings.TrimSpace(cfg.OpenILink.Token)
	cfg.OpenILink.TokenEnv = ""
	cfg.Telegram.SetEnabledExplicit(cfg.TelegramEnabled())
	cfg.Telegram.Token = strings.TrimSpace(cfg.Telegram.Token)
	cfg.Telegram.TokenEnv = ""
	cfg.Web.AdminPasswordEnv = ""
	for index := range cfg.Codex.Targets {
		cfg.Codex.Targets[index] = config.NormalizeTarget(cfg.Codex.Targets[index])
		cfg.Codex.Targets[index].APIKeyEnv = ""
		cfg.Codex.Targets[index].SortOrder = index
	}
	if err := cfg.ValidateAllowEmptyTargets(); err != nil {
		return fmt.Errorf("validate initial database configuration: %w", err)
	}
	codexData, err := json.Marshal(toPersistedCodex(cfg.Codex))
	if err != nil {
		return err
	}
	openData, err := json.Marshal(toPersistedOpenILink(cfg.OpenILink))
	if err != nil {
		return err
	}
	telegramData, err := json.Marshal(toPersistedTelegram(cfg.Telegram))
	if err != nil {
		return err
	}
	webData, err := json.Marshal(toPersistedWeb(cfg.Web))
	if err != nil {
		return err
	}
	var tokenCipher []byte
	if cfg.OpenILink.Token != "" {
		tokenCipher, err = s.encrypt([]byte(cfg.OpenILink.Token), "openilink:token")
		if err != nil {
			return err
		}
	}
	var telegramTokenCipher []byte
	if cfg.Telegram.Token != "" {
		telegramTokenCipher, err = s.encrypt([]byte(cfg.Telegram.Token), "telegram:token")
		if err != nil {
			return err
		}
	}
	masterKeyCipher, err := s.encrypt([]byte(masterKeyCheckPlaintext), masterKeyCheckAAD)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin initial configuration transaction: %w", err)
	}
	rollback := func(openErr error) error {
		_ = tx.Rollback()
		return openErr
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO app_meta(key, value) VALUES (?, ?)`, metaMasterKeyCheck, masterKeyCipher); err != nil {
		return rollback(fmt.Errorf("store master-key check: %w", err))
	}
	for _, section := range []struct {
		name   string
		data   []byte
		secret []byte
	}{
		{name: "codex", data: codexData},
		{name: "openilink", data: openData, secret: tokenCipher},
		{name: "telegram", data: telegramData, secret: telegramTokenCipher},
		{name: "web", data: webData},
	} {
		if _, err := tx.ExecContext(ctx, `INSERT INTO config_sections(name, revision, data, secret) VALUES (?, 1, ?, ?)`, section.name, string(section.data), nullableBytes(section.secret)); err != nil {
			return rollback(fmt.Errorf("store initial %s configuration: %w", section.name, err))
		}
	}
	for _, target := range cfg.Codex.Targets {
		if _, err := s.insertTargetTx(ctx, tx, target); err != nil {
			return rollback(fmt.Errorf("store initial target %q: %w", target.Name, err))
		}
	}
	for key, value := range map[string]string{
		metaInitialized:           "1",
		metaConfigRevision:        "1",
		metaLoadedStartupRevision: "0",
		metaSuggestedUsername:     suggestedUsername,
	} {
		if _, err := tx.ExecContext(ctx, `INSERT INTO app_meta(key, value) VALUES (?, ?)`, key, []byte(value)); err != nil {
			return rollback(fmt.Errorf("store initial metadata %s: %w", key, err))
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit initial configuration: %w", err)
	}
	return nil
}

func (s *Store) Load(ctx context.Context) (Snapshot, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Snapshot{}, fmt.Errorf("begin configuration read transaction: %w", err)
	}
	result, err := s.loadFrom(ctx, tx)
	if err != nil {
		_ = tx.Rollback()
		return Snapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return Snapshot{}, fmt.Errorf("commit configuration read transaction: %w", err)
	}
	return result, nil
}

func (s *Store) loadFrom(ctx context.Context, q queryer) (Snapshot, error) {
	result := Snapshot{SectionRevisions: make(map[string]int64, 4)}
	var codexRecord persistedCodex
	if revision, _, err := s.readSection(ctx, q, "codex", &codexRecord); err != nil {
		return Snapshot{}, err
	} else {
		result.SectionRevisions["codex"] = revision
	}
	var openRecord persistedOpenILink
	openRevision, tokenCipher, err := s.readSection(ctx, q, "openilink", &openRecord)
	if err != nil {
		return Snapshot{}, err
	}
	result.SectionRevisions["openilink"] = openRevision
	var telegramRecord persistedTelegram
	telegramRevision, telegramTokenCipher, err := s.readSection(ctx, q, "telegram", &telegramRecord)
	if err != nil {
		return Snapshot{}, err
	}
	result.SectionRevisions["telegram"] = telegramRevision
	var webRecord persistedWeb
	if revision, _, err := s.readSection(ctx, q, "web", &webRecord); err != nil {
		return Snapshot{}, err
	} else {
		result.SectionRevisions["web"] = revision
	}

	result.Config.Codex = fromPersistedCodex(codexRecord)
	result.Config.OpenILink = fromPersistedOpenILink(openRecord)
	result.Config.OpenILink.SetEnabledExplicit(openRecord.Enabled)
	if len(tokenCipher) > 0 {
		plain, decryptErr := s.decrypt(tokenCipher, "openilink:token")
		if decryptErr != nil {
			return Snapshot{}, fmt.Errorf("decrypt OpenILink token: %w", decryptErr)
		}
		result.Config.OpenILink.Token = string(plain)
	}
	result.Config.Telegram = fromPersistedTelegram(telegramRecord)
	result.Config.Telegram.SetEnabledExplicit(telegramRecord.Enabled)
	if len(telegramTokenCipher) > 0 {
		plain, decryptErr := s.decrypt(telegramTokenCipher, "telegram:token")
		if decryptErr != nil {
			return Snapshot{}, fmt.Errorf("decrypt Telegram token: %w", decryptErr)
		}
		result.Config.Telegram.Token = string(plain)
	}
	result.Config.Web = fromPersistedWeb(webRecord)

	rows, err := q.QueryContext(ctx, `SELECT id, sort_order, name, api_base_url, api_key, model, wire_api, config_overrides FROM targets ORDER BY sort_order, id`)
	if err != nil {
		return Snapshot{}, fmt.Errorf("read targets: %w", err)
	}
	for rows.Next() {
		var target config.Target
		var encrypted []byte
		var overrides string
		if err := rows.Scan(&target.ID, &target.SortOrder, &target.Name, &target.APIBaseURL, &encrypted, &target.Model, &target.WireAPI, &overrides); err != nil {
			_ = rows.Close()
			return Snapshot{}, fmt.Errorf("scan target: %w", err)
		}
		plain, err := s.decrypt(encrypted, targetAPIKeyAAD(target.ID))
		if err != nil {
			_ = rows.Close()
			return Snapshot{}, fmt.Errorf("decrypt target %q API key: %w", target.Name, err)
		}
		target.APIKey = string(plain)
		if err := json.Unmarshal([]byte(overrides), &target.ConfigOverrides); err != nil {
			_ = rows.Close()
			return Snapshot{}, fmt.Errorf("decode target %q overrides: %w", target.Name, err)
		}
		if target.ConfigOverrides == nil {
			target.ConfigOverrides = []string{}
		}
		result.Config.Codex.Targets = append(result.Config.Codex.Targets, target)
	}
	if err := rows.Close(); err != nil {
		return Snapshot{}, fmt.Errorf("close target rows: %w", err)
	}
	if result.Config.Codex.Targets == nil {
		result.Config.Codex.Targets = []config.Target{}
	}

	result.Revision, err = metaIntFrom(ctx, q, metaConfigRevision)
	if err != nil {
		return Snapshot{}, err
	}
	result.LoadedStartupRevision, err = metaIntFrom(ctx, q, metaLoadedStartupRevision)
	if err != nil {
		return Snapshot{}, err
	}
	result.SuggestedUsername, err = metaStringFrom(ctx, q, metaSuggestedUsername)
	if err != nil {
		return Snapshot{}, err
	}
	var username string
	err = q.QueryRowContext(ctx, `SELECT username FROM administrators WHERE singleton = 1`).Scan(&username)
	if errors.Is(err, sql.ErrNoRows) {
		result.SetupRequired = true
		result.Config.Web.AdminUsername = result.SuggestedUsername
	} else if err != nil {
		return Snapshot{}, fmt.Errorf("read administrator: %w", err)
	} else {
		result.Config.Web.AdminUsername = username
	}
	if err := result.Config.ValidateAllowEmptyTargets(); err != nil {
		return Snapshot{}, fmt.Errorf("validate stored configuration: %w", err)
	}
	return result, nil
}

func (s *Store) SetupAdmin(ctx context.Context, username, password string) (Snapshot, error) {
	username = strings.TrimSpace(username)
	if username == "" {
		return Snapshot{}, errors.New("administrator username is required")
	}
	if len([]rune(password)) < config.MinimumAdminPasswordLength {
		return Snapshot{}, fmt.Errorf("administrator password must be at least %d characters", config.MinimumAdminPasswordLength)
	}
	hash, err := hashPassword(password)
	if err != nil {
		return Snapshot{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Snapshot{}, err
	}
	var existing int
	err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM administrators`).Scan(&existing)
	if err != nil {
		_ = tx.Rollback()
		return Snapshot{}, fmt.Errorf("check administrator setup: %w", err)
	}
	if existing != 0 {
		_ = tx.Rollback()
		return Snapshot{}, ErrAlreadySetup
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO administrators(singleton, username, password_hash) VALUES (1, ?, ?)`, username, hash); err != nil {
		_ = tx.Rollback()
		if isUniqueError(err) {
			return Snapshot{}, ErrAlreadySetup
		}
		return Snapshot{}, fmt.Errorf("create administrator: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE app_meta SET value = ? WHERE key = ?`, []byte(username), metaSuggestedUsername); err != nil {
		_ = tx.Rollback()
		return Snapshot{}, err
	}
	if _, err := bumpRevisionTx(ctx, tx); err != nil {
		_ = tx.Rollback()
		return Snapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return Snapshot{}, err
	}
	return s.loadAfterCommit(ctx)
}

func (s *Store) VerifyAdmin(ctx context.Context, username, password string) (string, bool, error) {
	var storedUsername, encoded string
	err := s.db.QueryRowContext(ctx, `SELECT username, password_hash FROM administrators WHERE singleton = 1`).Scan(&storedUsername, &encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read administrator credentials: %w", err)
	}
	usernameHash := argon2.IDKey([]byte(strings.TrimSpace(username)), []byte("codex-queue-username"), 1, 8*1024, 1, 32)
	expectedUsernameHash := argon2.IDKey([]byte(storedUsername), []byte("codex-queue-username"), 1, 8*1024, 1, 32)
	usernameOK := subtle.ConstantTimeCompare(usernameHash, expectedUsernameHash) == 1
	passwordOK, err := verifyPassword(password, encoded)
	if err != nil {
		return "", false, fmt.Errorf("verify administrator password: %w", err)
	}
	return storedUsername, usernameOK && passwordOK, nil
}

func (s *Store) UpdateAccount(ctx context.Context, username, newPassword string) (Snapshot, error) {
	username = strings.TrimSpace(username)
	if username == "" {
		return Snapshot{}, errors.New("administrator username is required")
	}
	var newHash string
	var err error
	if newPassword != "" {
		if len([]rune(newPassword)) < config.MinimumAdminPasswordLength {
			return Snapshot{}, fmt.Errorf("administrator password must be at least %d characters", config.MinimumAdminPasswordLength)
		}
		newHash, err = hashPassword(newPassword)
		if err != nil {
			return Snapshot{}, err
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Snapshot{}, err
	}
	var currentHash string
	if err := tx.QueryRowContext(ctx, `SELECT password_hash FROM administrators WHERE singleton = 1`).Scan(&currentHash); err != nil {
		_ = tx.Rollback()
		if errors.Is(err, sql.ErrNoRows) {
			return Snapshot{}, ErrNotFound
		}
		return Snapshot{}, err
	}
	if newHash == "" {
		newHash = currentHash
	}
	if _, err := tx.ExecContext(ctx, `UPDATE administrators SET username = ?, password_hash = ? WHERE singleton = 1`, username, newHash); err != nil {
		_ = tx.Rollback()
		if isUniqueError(err) {
			return Snapshot{}, ErrConflict
		}
		return Snapshot{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE app_meta SET value = ? WHERE key = ?`, []byte(username), metaSuggestedUsername); err != nil {
		_ = tx.Rollback()
		return Snapshot{}, err
	}
	if _, err := bumpRevisionTx(ctx, tx); err != nil {
		_ = tx.Rollback()
		return Snapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return Snapshot{}, err
	}
	return s.loadAfterCommit(ctx)
}

func (s *Store) UpdateCodex(ctx context.Context, value config.CodexConfig) (Snapshot, error) {
	value.Binary = strings.TrimSpace(value.Binary)
	value.PromptsFile = strings.TrimSpace(value.PromptsFile)
	value.SuccessMessage = strings.TrimSpace(value.SuccessMessage)
	value.ReasoningEffort = strings.TrimSpace(value.ReasoningEffort)
	for index := range value.ConfigOverrides {
		value.ConfigOverrides[index] = strings.TrimSpace(value.ConfigOverrides[index])
	}
	value.Targets = nil
	if err := config.ValidateCodex(value); err != nil {
		return Snapshot{}, err
	}
	data, err := json.Marshal(toPersistedCodex(value))
	if err != nil {
		return Snapshot{}, err
	}
	if err := s.updateSection(ctx, "codex", data, nil, false); err != nil {
		return Snapshot{}, err
	}
	return s.loadAfterCommit(ctx)
}

func (s *Store) UpdateWeb(ctx context.Context, value config.WebConfig) (Snapshot, error) {
	value.ListenAddress = strings.TrimSpace(value.ListenAddress)
	for index := range value.TrustedProxies {
		value.TrustedProxies[index] = strings.TrimSpace(value.TrustedProxies[index])
	}
	if err := config.ValidateWeb(value); err != nil {
		return Snapshot{}, err
	}
	data, err := json.Marshal(toPersistedWeb(value))
	if err != nil {
		return Snapshot{}, err
	}
	if err := s.updateSection(ctx, "web", data, nil, false); err != nil {
		return Snapshot{}, err
	}
	return s.loadAfterCommit(ctx)
}

// UpdateOpenILink keeps the existing token when token is nil. A non-nil empty
// token clears it; callers enforce that clearing is only allowed while disabled.
func (s *Store) UpdateOpenILink(ctx context.Context, value config.OpenILinkConfig, token *string) (Snapshot, error) {
	value.SetEnabledExplicit(value.Enabled)
	value.BaseURL = strings.TrimRight(strings.TrimSpace(value.BaseURL), "/")
	for index := range value.AllowedUserIDs {
		value.AllowedUserIDs[index] = strings.TrimSpace(value.AllowedUserIDs[index])
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Snapshot{}, err
	}
	var currentCipher []byte
	if err := tx.QueryRowContext(ctx, `SELECT secret FROM config_sections WHERE name = 'openilink'`).Scan(&currentCipher); err != nil {
		_ = tx.Rollback()
		return Snapshot{}, err
	}
	currentToken := ""
	if len(currentCipher) > 0 {
		plain, decryptErr := s.decrypt(currentCipher, "openilink:token")
		if decryptErr != nil {
			_ = tx.Rollback()
			return Snapshot{}, decryptErr
		}
		currentToken = string(plain)
	}
	if token != nil {
		trimmed := strings.TrimSpace(*token)
		if trimmed != "" || *token == "" {
			currentToken = trimmed
		}
	}
	value.Token = currentToken
	value.TokenEnv = ""
	if err := config.ValidateOpenILink(value); err != nil {
		_ = tx.Rollback()
		return Snapshot{}, err
	}
	data, err := json.Marshal(toPersistedOpenILink(value))
	if err != nil {
		_ = tx.Rollback()
		return Snapshot{}, err
	}
	var cipherValue []byte
	if currentToken != "" {
		cipherValue, err = s.encrypt([]byte(currentToken), "openilink:token")
		if err != nil {
			_ = tx.Rollback()
			return Snapshot{}, err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE config_sections SET revision = revision + 1, data = ?, secret = ? WHERE name = 'openilink'`, string(data), nullableBytes(cipherValue)); err != nil {
		_ = tx.Rollback()
		return Snapshot{}, err
	}
	if _, err := bumpRevisionTx(ctx, tx); err != nil {
		_ = tx.Rollback()
		return Snapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return Snapshot{}, err
	}
	return s.loadAfterCommit(ctx)
}

// UpdateTelegram keeps the existing token when token is nil. A non-nil empty
// token clears it; callers enforce that clearing is only allowed while disabled.
func (s *Store) UpdateTelegram(ctx context.Context, value config.TelegramConfig, token *string) (Snapshot, error) {
	value.SetEnabledExplicit(value.Enabled)
	value.BaseURL = strings.TrimRight(strings.TrimSpace(value.BaseURL), "/")
	for index := range value.AllowedUserIDs {
		value.AllowedUserIDs[index] = strings.TrimSpace(value.AllowedUserIDs[index])
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Snapshot{}, err
	}
	var currentCipher []byte
	if err := tx.QueryRowContext(ctx, `SELECT secret FROM config_sections WHERE name = 'telegram'`).Scan(&currentCipher); err != nil {
		_ = tx.Rollback()
		return Snapshot{}, err
	}
	currentToken := ""
	if len(currentCipher) > 0 {
		plain, decryptErr := s.decrypt(currentCipher, "telegram:token")
		if decryptErr != nil {
			_ = tx.Rollback()
			return Snapshot{}, decryptErr
		}
		currentToken = string(plain)
	}
	if token != nil {
		currentToken = strings.TrimSpace(*token)
	}
	value.Token = currentToken
	value.TokenEnv = ""
	if err := config.ValidateTelegram(value); err != nil {
		_ = tx.Rollback()
		return Snapshot{}, err
	}
	data, err := json.Marshal(toPersistedTelegram(value))
	if err != nil {
		_ = tx.Rollback()
		return Snapshot{}, err
	}
	var cipherValue []byte
	if currentToken != "" {
		cipherValue, err = s.encrypt([]byte(currentToken), "telegram:token")
		if err != nil {
			_ = tx.Rollback()
			return Snapshot{}, err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE config_sections SET revision = revision + 1, data = ?, secret = ? WHERE name = 'telegram'`, string(data), nullableBytes(cipherValue)); err != nil {
		_ = tx.Rollback()
		return Snapshot{}, err
	}
	if _, err := bumpRevisionTx(ctx, tx); err != nil {
		_ = tx.Rollback()
		return Snapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return Snapshot{}, err
	}
	return s.loadAfterCommit(ctx)
}

func (s *Store) CreateTarget(ctx context.Context, target config.Target) (Snapshot, config.Target, error) {
	target = config.NormalizeTarget(target)
	if target.SortOrder < 0 {
		var maximum sql.NullInt64
		if err := s.db.QueryRowContext(ctx, `SELECT MAX(sort_order) FROM targets`).Scan(&maximum); err != nil {
			return Snapshot{}, config.Target{}, err
		}
		if maximum.Valid {
			target.SortOrder = int(maximum.Int64 + 1)
		} else {
			target.SortOrder = 0
		}
	}
	if err := config.ValidateTarget(target); err != nil {
		return Snapshot{}, config.Target{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Snapshot{}, config.Target{}, err
	}
	created, err := s.insertTargetTx(ctx, tx, target)
	if err != nil {
		_ = tx.Rollback()
		if isUniqueError(err) {
			return Snapshot{}, config.Target{}, ErrConflict
		}
		return Snapshot{}, config.Target{}, err
	}
	if _, err := bumpRevisionTx(ctx, tx); err != nil {
		_ = tx.Rollback()
		return Snapshot{}, config.Target{}, err
	}
	if err := tx.Commit(); err != nil {
		return Snapshot{}, config.Target{}, err
	}
	snapshot, err := s.loadAfterCommit(ctx)
	return snapshot, created, err
}

func (s *Store) UpdateTarget(ctx context.Context, id int64, target config.Target, apiKey *string) (Snapshot, config.Target, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Snapshot{}, config.Target{}, err
	}
	var currentSort int
	var currentCipher []byte
	if err := tx.QueryRowContext(ctx, `SELECT sort_order, api_key FROM targets WHERE id = ?`, id).Scan(&currentSort, &currentCipher); err != nil {
		_ = tx.Rollback()
		if errors.Is(err, sql.ErrNoRows) {
			return Snapshot{}, config.Target{}, ErrNotFound
		}
		return Snapshot{}, config.Target{}, err
	}
	plain, err := s.decrypt(currentCipher, targetAPIKeyAAD(id))
	if err != nil {
		_ = tx.Rollback()
		return Snapshot{}, config.Target{}, err
	}
	target = config.NormalizeTarget(target)
	target.ID = id
	if target.SortOrder < 0 {
		target.SortOrder = currentSort
	}
	target.APIKey = string(plain)
	if apiKey != nil && strings.TrimSpace(*apiKey) != "" {
		target.APIKey = strings.TrimSpace(*apiKey)
	}
	if err := config.ValidateTarget(target); err != nil {
		_ = tx.Rollback()
		return Snapshot{}, config.Target{}, err
	}
	encrypted, err := s.encrypt([]byte(target.APIKey), targetAPIKeyAAD(id))
	if err != nil {
		_ = tx.Rollback()
		return Snapshot{}, config.Target{}, err
	}
	overrides, err := json.Marshal(nonNil(target.ConfigOverrides))
	if err != nil {
		_ = tx.Rollback()
		return Snapshot{}, config.Target{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE targets SET sort_order = ?, revision = revision + 1, name = ?, api_base_url = ?, api_key = ?, model = ?, wire_api = ?, config_overrides = ? WHERE id = ?`, target.SortOrder, target.Name, target.APIBaseURL, encrypted, target.Model, target.WireAPI, string(overrides), id); err != nil {
		_ = tx.Rollback()
		if isUniqueError(err) {
			return Snapshot{}, config.Target{}, ErrConflict
		}
		return Snapshot{}, config.Target{}, err
	}
	if _, err := bumpRevisionTx(ctx, tx); err != nil {
		_ = tx.Rollback()
		return Snapshot{}, config.Target{}, err
	}
	if err := tx.Commit(); err != nil {
		return Snapshot{}, config.Target{}, err
	}
	snapshot, err := s.loadAfterCommit(ctx)
	return snapshot, target, err
}

func (s *Store) DeleteTarget(ctx context.Context, id int64) (Snapshot, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Snapshot{}, err
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM targets WHERE id = ?`, id)
	if err != nil {
		_ = tx.Rollback()
		return Snapshot{}, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		_ = tx.Rollback()
		return Snapshot{}, err
	}
	if affected == 0 {
		_ = tx.Rollback()
		return Snapshot{}, ErrNotFound
	}
	if _, err := bumpRevisionTx(ctx, tx); err != nil {
		_ = tx.Rollback()
		return Snapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return Snapshot{}, err
	}
	return s.loadAfterCommit(ctx)
}

// Once a transaction commits, finish rebuilding the returned snapshot even if
// the HTTP client that initiated the write disconnects. Otherwise persistence
// could succeed while the in-memory manager incorrectly treats the write as a
// failure until the next process restart.
func (s *Store) loadAfterCommit(ctx context.Context) (Snapshot, error) {
	return s.Load(context.WithoutCancel(ctx))
}

func (s *Store) MarkStartupLoaded(ctx context.Context, revision int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE app_meta SET value = ? WHERE key = ?`, []byte(strconv.FormatInt(revision, 10)), metaLoadedStartupRevision)
	if err != nil {
		return fmt.Errorf("record loaded startup revision: %w", err)
	}
	return nil
}

func (s *Store) updateSection(ctx context.Context, name string, data []byte, secret []byte, replaceSecret bool) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	var result sql.Result
	if replaceSecret {
		result, err = tx.ExecContext(ctx, `UPDATE config_sections SET revision = revision + 1, data = ?, secret = ? WHERE name = ?`, string(data), nullableBytes(secret), name)
	} else {
		result, err = tx.ExecContext(ctx, `UPDATE config_sections SET revision = revision + 1, data = ? WHERE name = ?`, string(data), name)
	}
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		_ = tx.Rollback()
		return ErrNotFound
	}
	if _, err := bumpRevisionTx(ctx, tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *Store) insertTargetTx(ctx context.Context, tx *sql.Tx, target config.Target) (config.Target, error) {
	overrides, err := json.Marshal(nonNil(target.ConfigOverrides))
	if err != nil {
		return config.Target{}, err
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO targets(sort_order, revision, name, api_base_url, api_key, model, wire_api, config_overrides) VALUES (?, 1, ?, ?, ?, ?, ?, ?)`, target.SortOrder, target.Name, target.APIBaseURL, []byte{}, target.Model, target.WireAPI, string(overrides))
	if err != nil {
		return config.Target{}, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return config.Target{}, err
	}
	encrypted, err := s.encrypt([]byte(target.APIKey), targetAPIKeyAAD(id))
	if err != nil {
		return config.Target{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE targets SET api_key = ? WHERE id = ?`, encrypted, id); err != nil {
		return config.Target{}, err
	}
	target.ID = id
	return target, nil
}

func (s *Store) readSection(ctx context.Context, q queryer, name string, destination any) (int64, []byte, error) {
	var revision int64
	var data string
	var secret []byte
	if err := q.QueryRowContext(ctx, `SELECT revision, data, secret FROM config_sections WHERE name = ?`, name).Scan(&revision, &data, &secret); err != nil {
		return 0, nil, fmt.Errorf("read %s configuration: %w", name, err)
	}
	if err := json.Unmarshal([]byte(data), destination); err != nil {
		return 0, nil, fmt.Errorf("decode %s configuration: %w", name, err)
	}
	return revision, secret, nil
}

func (s *Store) metaBool(ctx context.Context, key string) (bool, error) {
	value, err := metaStringOptionalFrom(ctx, s.db, key)
	if err != nil {
		return false, err
	}
	return value == "1" || strings.EqualFold(value, "true"), nil
}

func metaIntFrom(ctx context.Context, q queryer, key string) (int64, error) {
	value, err := metaStringFrom(ctx, q, key)
	if err != nil {
		return 0, err
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse metadata %s: %w", key, err)
	}
	return parsed, nil
}

func metaStringFrom(ctx context.Context, q queryer, key string) (string, error) {
	value, err := metaStringOptionalFrom(ctx, q, key)
	if err != nil {
		return "", err
	}
	if value == "" {
		return "", fmt.Errorf("metadata %s is missing", key)
	}
	return value, nil
}

func metaStringOptionalFrom(ctx context.Context, q queryer, key string) (string, error) {
	var value []byte
	err := q.QueryRowContext(ctx, `SELECT value FROM app_meta WHERE key = ?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read metadata %s: %w", key, err)
	}
	return string(value), nil
}

func bumpRevisionTx(ctx context.Context, tx *sql.Tx) (int64, error) {
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT value FROM app_meta WHERE key = ?`, metaConfigRevision).Scan(&raw); err != nil {
		return 0, fmt.Errorf("read configuration revision: %w", err)
	}
	current, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse configuration revision: %w", err)
	}
	current++
	if _, err := tx.ExecContext(ctx, `UPDATE app_meta SET value = ? WHERE key = ?`, []byte(strconv.FormatInt(current, 10)), metaConfigRevision); err != nil {
		return 0, fmt.Errorf("update configuration revision: %w", err)
	}
	return current, nil
}

func (s *Store) encrypt(plain []byte, aad string) ([]byte, error) {
	block, err := aes.NewCipher(s.key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("generate encryption nonce: %w", err)
	}
	result := make([]byte, 1+len(nonce), 1+len(nonce)+len(plain)+gcm.Overhead())
	result[0] = cipherVersion
	copy(result[1:], nonce)
	result = gcm.Seal(result, nonce, plain, []byte(aad))
	return result, nil
}

func (s *Store) decrypt(value []byte, aad string) ([]byte, error) {
	block, err := aes.NewCipher(s.key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(value) < 1+gcm.NonceSize()+gcm.Overhead() || value[0] != cipherVersion {
		return nil, errors.New("encrypted value has an unsupported format")
	}
	nonce := value[1 : 1+gcm.NonceSize()]
	plain, err := gcm.Open(nil, nonce, value[1+gcm.NonceSize():], []byte(aad))
	if err != nil {
		return nil, errors.New("encrypted value authentication failed")
	}
	return plain, nil
}

func hashPassword(password string) (string, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("generate password salt: %w", err)
	}
	const memory = 32 * 1024
	const iterations = 2
	const parallelism = 1
	const keyLength = 32
	hash := argon2.IDKey([]byte(password), salt, iterations, memory, parallelism, keyLength)
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s", argon2.Version, memory, iterations, parallelism, base64.RawStdEncoding.EncodeToString(salt), base64.RawStdEncoding.EncodeToString(hash)), nil
}

func verifyPassword(password, encoded string) (bool, error) {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[1] != "argon2id" {
		return false, errors.New("invalid Argon2id password hash")
	}
	var version, memory, iterations uint32
	var parallelism uint8
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil || version != argon2.Version {
		return false, errors.New("unsupported Argon2id version")
	}
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &memory, &iterations, &parallelism); err != nil {
		return false, errors.New("invalid Argon2id parameters")
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false, errors.New("invalid Argon2id salt")
	}
	expected, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return false, errors.New("invalid Argon2id digest")
	}
	actual := argon2.IDKey([]byte(password), salt, iterations, memory, parallelism, uint32(len(expected)))
	return subtle.ConstantTimeCompare(actual, expected) == 1, nil
}

func targetAPIKeyAAD(id int64) string {
	buffer := make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, uint64(id))
	return "target:" + base64.RawURLEncoding.EncodeToString(buffer) + ":api_key"
}

func nullableBytes(value []byte) any {
	if len(value) == 0 {
		return nil
	}
	return value
}

func nonNil(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}

func isUniqueError(err error) bool {
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "unique constraint") || strings.Contains(text, "constraint failed")
}

func protectedDirectory(path string) bool {
	abs, err := filepath.Abs(path)
	if err != nil {
		return true
	}
	protected := []string{string(os.PathSeparator), filepath.Clean(os.TempDir())}
	if home, err := os.UserHomeDir(); err == nil {
		protected = append(protected, filepath.Clean(home))
	}
	if cwd, err := os.Getwd(); err == nil {
		protected = append(protected, filepath.Clean(cwd))
	}
	for _, item := range protected {
		if abs == item {
			return true
		}
	}
	return false
}

func toPersistedCodex(value config.CodexConfig) persistedCodex {
	return persistedCodex{
		Binary: value.Binary, PromptsFile: value.PromptsFile,
		RequestTimeoutSecond: value.RequestTimeoutSecond,
		RetryMinSecond:       value.RetryMinSecond, RetryMaxSecond: value.RetryMaxSecond,
		KeepaliveMinSecond: value.KeepaliveMinSecond, KeepaliveMaxSecond: value.KeepaliveMaxSecond,
		MaxParallel: value.MaxParallel, SuccessMessage: value.SuccessMessage,
		ReasoningEffort: value.ReasoningEffort, ConfigOverrides: nonNil(value.ConfigOverrides),
	}
}

func fromPersistedCodex(value persistedCodex) config.CodexConfig {
	return config.CodexConfig{
		Binary: value.Binary, PromptsFile: value.PromptsFile,
		RequestTimeoutSecond: value.RequestTimeoutSecond,
		RetryMinSecond:       value.RetryMinSecond, RetryMaxSecond: value.RetryMaxSecond,
		KeepaliveMinSecond: value.KeepaliveMinSecond, KeepaliveMaxSecond: value.KeepaliveMaxSecond,
		MaxParallel: value.MaxParallel, SuccessMessage: value.SuccessMessage,
		ReasoningEffort: value.ReasoningEffort, ConfigOverrides: nonNil(value.ConfigOverrides), Targets: []config.Target{},
	}
}

func toPersistedOpenILink(value config.OpenILinkConfig) persistedOpenILink {
	return persistedOpenILink{Enabled: value.Enabled, BaseURL: value.BaseURL, AllowedUserIDs: nonNil(value.AllowedUserIDs), HTTPTimeoutSecond: value.HTTPTimeoutSecond}
}

func fromPersistedOpenILink(value persistedOpenILink) config.OpenILinkConfig {
	return config.OpenILinkConfig{Enabled: value.Enabled, BaseURL: value.BaseURL, AllowedUserIDs: nonNil(value.AllowedUserIDs), HTTPTimeoutSecond: value.HTTPTimeoutSecond}
}

func toPersistedTelegram(value config.TelegramConfig) persistedTelegram {
	return persistedTelegram{
		Enabled: value.Enabled, BaseURL: value.BaseURL, AllowedUserIDs: nonNil(value.AllowedUserIDs),
		HTTPTimeoutSecond: value.HTTPTimeoutSecond, PollTimeoutSecond: value.PollTimeoutSecond,
	}
}

func fromPersistedTelegram(value persistedTelegram) config.TelegramConfig {
	return config.TelegramConfig{
		Enabled: value.Enabled, BaseURL: value.BaseURL, AllowedUserIDs: nonNil(value.AllowedUserIDs),
		HTTPTimeoutSecond: value.HTTPTimeoutSecond, PollTimeoutSecond: value.PollTimeoutSecond,
	}
}

func toPersistedWeb(value config.WebConfig) persistedWeb {
	return persistedWeb{ListenAddress: value.ListenAddress, CookieSecure: value.CookieSecure, TrustedProxies: nonNil(value.TrustedProxies), ActivityLimit: value.ActivityLimit}
}

func fromPersistedWeb(value persistedWeb) config.WebConfig {
	return config.WebConfig{ListenAddress: value.ListenAddress, CookieSecure: value.CookieSecure, TrustedProxies: nonNil(value.TrustedProxies), ActivityLimit: value.ActivityLimit}
}
