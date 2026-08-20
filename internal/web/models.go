package web

import (
	"time"

	"codex-queue-bot/internal/hub"
	"codex-queue-bot/internal/jobs"
)

type dashboardResponse struct {
	Version     string              `json:"version"`
	GeneratedAt time.Time           `json:"generated_at"`
	OpenILink   openILinkResponse   `json:"openilink"`
	Concurrency concurrencyResponse `json:"concurrency"`
	Targets     []targetResponse    `json:"targets"`
	Activities  []activityResponse  `json:"activities"`
}

type stateResponse struct {
	Version     string              `json:"version"`
	GeneratedAt time.Time           `json:"generated_at"`
	OpenILink   openILinkResponse   `json:"openilink"`
	Concurrency concurrencyResponse `json:"concurrency"`
	Targets     []targetResponse    `json:"targets"`
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
	Name      string            `json:"name"`
	Model     string            `json:"model"`
	APIHost   string            `json:"api_host"`
	Queue     queueResponse     `json:"queue"`
	Keepalive keepaliveResponse `json:"keepalive"`
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

type activityResponse struct {
	ID       uint64      `json:"id"`
	Type     string      `json:"type"`
	Target   string      `json:"target"`
	Source   jobs.Source `json:"source"`
	Actor    string      `json:"actor"`
	Attempts int         `json:"attempts"`
	At       time.Time   `json:"at"`
	Error    string      `json:"error,omitempty"`
}

func (s *Server) dashboardPayload(snapshot jobs.ManagerSnapshot, activities []jobs.Activity, status hub.StatusSnapshot) dashboardResponse {
	state := s.statePayload(snapshot, status)
	result := dashboardResponse{
		Version:     state.Version,
		GeneratedAt: state.GeneratedAt,
		OpenILink:   state.OpenILink,
		Concurrency: state.Concurrency,
		Targets:     state.Targets,
		Activities:  make([]activityResponse, 0, len(activities)),
	}
	for _, activity := range activities {
		result.Activities = append(result.Activities, makeActivityResponse(activity))
	}
	return result
}

func (s *Server) statePayload(snapshot jobs.ManagerSnapshot, status hub.StatusSnapshot) stateResponse {
	result := stateResponse{
		Version:     s.version,
		GeneratedAt: s.now(),
		OpenILink: openILinkResponse{
			State:     status.State,
			Error:     status.Error,
			UpdatedAt: timePointer(status.UpdatedAt),
		},
		Concurrency: concurrencyResponse{Current: snapshot.CurrentProcesses, Max: snapshot.MaxParallel},
		Targets:     make([]targetResponse, 0, len(snapshot.Targets)),
	}
	for _, target := range snapshot.Targets {
		result.Targets = append(result.Targets, targetResponse{
			Name:    target.Name,
			Model:   target.Model,
			APIHost: target.APIHost,
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

func makeActivityResponse(activity jobs.Activity) activityResponse {
	return activityResponse{
		ID:       activity.ID,
		Type:     activity.Type,
		Target:   activity.Target,
		Source:   activity.Source,
		Actor:    activity.Actor,
		Attempts: activity.Attempts,
		At:       activity.At,
		Error:    activity.Error,
	}
}

func timePointer(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	copy := value
	return &copy
}
