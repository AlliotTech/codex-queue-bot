package web

import (
	"time"

	"codex-queue-bot/internal/hub"
	"codex-queue-bot/internal/jobs"
)

type dashboardResponse struct {
	Version         string              `json:"version"`
	GeneratedAt     time.Time           `json:"generated_at"`
	ConfigRevision  int64               `json:"config_revision"`
	RestartRequired bool                `json:"restart_required"`
	RestartFields   []string            `json:"restart_fields"`
	OpenILink       openILinkResponse   `json:"openilink"`
	Telegram        openILinkResponse   `json:"telegram"`
	Concurrency     concurrencyResponse `json:"concurrency"`
	Targets         []targetResponse    `json:"targets"`
}

type stateResponse struct {
	Version         string              `json:"version"`
	GeneratedAt     time.Time           `json:"generated_at"`
	ConfigRevision  int64               `json:"config_revision"`
	RestartRequired bool                `json:"restart_required"`
	RestartFields   []string            `json:"restart_fields"`
	OpenILink       openILinkResponse   `json:"openilink"`
	Telegram        openILinkResponse   `json:"telegram"`
	Concurrency     concurrencyResponse `json:"concurrency"`
	Targets         []targetResponse    `json:"targets"`
}

type openILinkResponse struct {
	State     hub.Status `json:"state"`
	Error     string     `json:"error,omitempty"`
	UpdatedAt *time.Time `json:"updated_at"`
}

type concurrencyResponse struct {
	Current int `json:"current"`
	Max     int `json:"max"`
}

type targetResponse struct {
	ID           int64             `json:"id"`
	Name         string            `json:"name"`
	Model        string            `json:"model"`
	APIHost      string            `json:"api_host"`
	Busy         bool              `json:"busy"`
	AdhocRunning bool              `json:"adhoc_running"`
	Queue        queueResponse     `json:"queue"`
	Keepalive    keepaliveResponse `json:"keepalive"`
}

type queueResponse struct {
	State       jobs.State `json:"state"`
	Attempts    int        `json:"attempts"`
	StartedAt   *time.Time `json:"started_at"`
	LastAttempt *time.Time `json:"last_attempt"`
	NextAttempt *time.Time `json:"next_attempt"`
	FinishedAt  *time.Time `json:"finished_at"`
	LastError   string     `json:"last_error,omitempty"`
}

type keepaliveResponse struct {
	State       jobs.KeepaliveState `json:"state"`
	Requests    int                 `json:"requests"`
	StartedAt   *time.Time          `json:"started_at"`
	LastRequest *time.Time          `json:"last_request"`
	LastSuccess *time.Time          `json:"last_success"`
	LastFailure *time.Time          `json:"last_failure"`
	NextRequest *time.Time          `json:"next_request"`
	StoppedAt   *time.Time          `json:"stopped_at"`
	LastError   string              `json:"last_error,omitempty"`
}

func (s *Server) dashboardPayload(snapshot jobs.ManagerSnapshot, openILinkStatus, telegramStatus hub.StatusSnapshot) dashboardResponse {
	state := s.statePayload(snapshot, openILinkStatus, telegramStatus)
	return dashboardResponse{
		Version: state.Version, GeneratedAt: state.GeneratedAt,
		ConfigRevision: state.ConfigRevision, RestartRequired: state.RestartRequired, RestartFields: state.RestartFields,
		OpenILink: state.OpenILink, Telegram: state.Telegram, Concurrency: state.Concurrency, Targets: state.Targets,
	}
}

func (s *Server) statePayload(snapshot jobs.ManagerSnapshot, openILinkStatus, telegramStatus hub.StatusSnapshot) stateResponse {
	configuration := s.currentConfiguration()
	restartFields := s.restartFields(configuration.Config)
	result := stateResponse{
		Version: s.version, GeneratedAt: s.now(), ConfigRevision: configuration.Revision,
		RestartRequired: len(restartFields) > 0, RestartFields: restartFields,
		OpenILink: openILinkResponse{
			State:     openILinkStatus.State,
			Error:     openILinkStatus.Error,
			UpdatedAt: timePointer(openILinkStatus.UpdatedAt),
		},
		Telegram: openILinkResponse{
			State:     telegramStatus.State,
			Error:     telegramStatus.Error,
			UpdatedAt: timePointer(telegramStatus.UpdatedAt),
		},
		Concurrency: concurrencyResponse{Current: snapshot.CurrentProcesses, Max: snapshot.MaxParallel},
		Targets:     make([]targetResponse, 0, len(snapshot.Targets)),
	}
	for _, target := range snapshot.Targets {
		result.Targets = append(result.Targets, targetResponse{
			ID:           target.ID,
			Name:         target.Name,
			Model:        target.Model,
			APIHost:      target.APIHost,
			Busy:         target.Busy,
			AdhocRunning: target.AdhocRunning,
			Queue: queueResponse{
				State:       target.Queue.State,
				Attempts:    target.Queue.Attempts,
				StartedAt:   timePointer(target.Queue.StartedAt),
				LastAttempt: timePointer(target.Queue.LastAttempt),
				NextAttempt: timePointer(target.Queue.NextAttempt),
				FinishedAt:  timePointer(target.Queue.FinishedAt),
				LastError:   target.Queue.LastError,
			},
			Keepalive: keepaliveResponse{
				State:       target.Keepalive.State,
				Requests:    target.Keepalive.Requests,
				StartedAt:   timePointer(target.Keepalive.StartedAt),
				LastRequest: timePointer(target.Keepalive.LastRequest),
				LastSuccess: timePointer(target.Keepalive.LastSuccess),
				LastFailure: timePointer(target.Keepalive.LastFailure),
				NextRequest: timePointer(target.Keepalive.NextRequest),
				StoppedAt:   timePointer(target.Keepalive.StoppedAt),
				LastError:   target.Keepalive.LastError,
			},
		})
	}
	return result
}

func timePointer(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	copy := value
	return &copy
}
