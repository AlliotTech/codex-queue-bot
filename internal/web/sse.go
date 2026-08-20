package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"codex-queue-bot/internal/jobs"

	"github.com/gin-gonic/gin"
)

func (s *Server) events(c *gin.Context) {
	currentValue, _ := c.Get("session")
	current := currentValue.(session)
	initialState, initialActivities, managerSubscription := s.manager.Observe(s.observerBuffer)
	defer managerSubscription.Close()
	initialStatus, statusSubscription := s.status.Observe(8)
	defer statusSubscription.Close()

	c.Header("Content-Type", "text/event-stream; charset=utf-8")
	c.Header("Cache-Control", "no-cache, no-transform")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")
	c.Status(http.StatusOK)
	_, _ = fmt.Fprint(c.Writer, "retry: 3000\n\n")
	if !s.writeSSE(c, "snapshot", "", s.dashboardPayload(initialState, initialActivities, initialStatus)) {
		return
	}

	heartbeat := time.NewTicker(s.heartbeat)
	defer heartbeat.Stop()
	expiresIn := current.ExpiresAt.Sub(s.now())
	if expiresIn < 0 {
		expiresIn = 0
	}
	expires := time.NewTimer(expiresIn)
	defer expires.Stop()

	for {
		select {
		case <-c.Request.Context().Done():
			return
		case <-s.shutdown:
			_ = s.writeSSE(c, "shutdown", "", map[string]any{"at": s.now()})
			return
		case <-expires.C:
			_ = s.writeSSE(c, "auth_expired", "", map[string]any{"at": s.now()})
			return
		case event, ok := <-managerSubscription.Events:
			if !ok {
				return
			}
			switch event.Kind {
			case jobs.EventState:
				if event.Snapshot != nil && !s.writeSSE(c, "state", fmt.Sprint(event.ID), s.statePayload(*event.Snapshot, s.status.Snapshot())) {
					return
				}
			case jobs.EventActivity:
				if event.Activity != nil && !s.writeSSE(c, "activity", fmt.Sprint(event.ID), makeActivityResponse(*event.Activity)) {
					return
				}
			}
		case status, ok := <-statusSubscription.Updates:
			if !ok {
				return
			}
			if !s.writeSSE(c, "state", "", s.statePayload(s.manager.ComprehensiveSnapshot(), status)) {
				return
			}
		case at := <-heartbeat.C:
			if !s.writeSSE(c, "heartbeat", "", map[string]time.Time{"at": at}) {
				return
			}
		}
	}
}

func (s *Server) writeSSE(c *gin.Context, event, id string, data any) bool {
	payload, err := json.Marshal(data)
	if err != nil {
		s.logger.Error("failed to encode SSE event", "event", event, "error", err)
		return false
	}
	controller := http.NewResponseController(c.Writer)
	deadline := 30 * time.Second
	if s.heartbeat > 0 && s.heartbeat*2 > deadline {
		deadline = s.heartbeat * 2
	}
	_ = controller.SetWriteDeadline(time.Now().Add(deadline))
	if id != "" {
		if _, err := fmt.Fprintf(c.Writer, "id: %s\n", id); err != nil {
			return false
		}
	}
	if _, err := fmt.Fprintf(c.Writer, "event: %s\ndata: %s\n\n", event, payload); err != nil {
		return false
	}
	c.Writer.Flush()
	return true
}
