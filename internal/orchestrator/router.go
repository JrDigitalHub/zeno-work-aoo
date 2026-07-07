package orchestrator

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/JrDigitalHub/zeno-work-aoo/internal/memory"
	"github.com/JrDigitalHub/zeno-work-aoo/pkg/protocol"
	"github.com/redis/go-redis/v9"
	"github.com/riverqueue/river"
)

type EventHandler func(ctx context.Context, event protocol.Event) error

type EventRouter struct {
	client      *redis.Client // Keep for cache/session fallback compatibility if needed
	riverClient *river.Client
	subscribers []EventHandler
	ctx         context.Context
	mu          sync.RWMutex
}

func NewEventRouter() *EventRouter {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "localhost:6379" // Default local Docker port
	}

	// Connect to Redis (used for caching/sessions other than this pub/sub)
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisURL,
		Password: "",
		DB:       0,
		PoolSize: 10, // Small connection pool for non-pub/sub cache
	})

	return &EventRouter{
		client:      rdb,
		subscribers: make([]EventHandler, 0),
		ctx:         context.Background(),
	}
}

func (r *EventRouter) SetRiverClient(client *river.Client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.riverClient = client
}

func (r *EventRouter) Subscribe(handler EventHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.subscribers = append(r.subscribers, handler)
}

func (r *EventRouter) Start() {
	fmt.Println("🛡️ [ROUTER] River Background Job Engine online. Awaiting telemetry...")
	// Redis Pub/Sub subscription loop is replaced entirely with River worker loops.
	// We no longer spawn subscribers via redis Pub/Sub inside router.go.
}

func (r *EventRouter) Dispatch(ctx context.Context, e protocol.Event) error {
	r.mu.RLock()
	subs := make([]EventHandler, len(r.subscribers))
	copy(subs, r.subscribers)
	r.mu.RUnlock()

	var errs []error
	for _, handler := range subs {
		if err := r.runHandler(ctx, handler, e); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("dispatch errors: %v", errs)
	}
	return nil
}

func (r *EventRouter) runHandler(ctx context.Context, handler EventHandler, e protocol.Event) (err error) {
	defer func() {
		if p := recover(); p != nil {
			slog.Error("Recovered from panic in event handler", slog.Any("panic", p), slog.String("source", e.Source))
			err = fmt.Errorf("panic in handler: %v", p)
		}
	}()
	return handler(ctx, e)
}

func (r *EventRouter) Publish(ctx context.Context, e protocol.Event) error {
	r.mu.RLock()
	rc := r.riverClient
	r.mu.RUnlock()

	if rc == nil {
		env := os.Getenv("APP_ENV")
		if env == "production" || env == "prod" {
			return fmt.Errorf("database/River client is offline in production environment; cannot publish event")
		}
		slog.Warn("DB/River offline, executing subscribers inline (development fallback)", slog.String("source", e.Source))
		return r.Dispatch(ctx, e)
	}

	var args river.JobArgs
	switch e.Source {
	case "DISCOVERY":
		args = DiscoveryJobArgs{
			WorkspaceID: e.WorkspaceID,
			ID:          e.ID,
			Source:      e.Source,
			Target:      e.Target,
			Payload:     e.Payload,
			Timestamp:   e.Timestamp,
		}
	case "PREDATOR":
		args = PredatorJobArgs{
			WorkspaceID: e.WorkspaceID,
			ID:          e.ID,
			Source:      e.Source,
			Target:      e.Target,
			Payload:     e.Payload,
			Timestamp:   e.Timestamp,
		}
	case "SENTINEL_TEXT_OUTPUT":
		args = SentinelTextOutputJobArgs{
			WorkspaceID: e.WorkspaceID,
			ID:          e.ID,
			Source:      e.Source,
			Target:      e.Target,
			Payload:     e.Payload,
			Timestamp:   e.Timestamp,
		}
	case "MODELER_RESULT":
		args = ModelerResultJobArgs{
			WorkspaceID: e.WorkspaceID,
			ID:          e.ID,
			Source:      e.Source,
			Target:      e.Target,
			Payload:     e.Payload,
			Timestamp:   e.Timestamp,
		}
	default:
		env := os.Getenv("APP_ENV")
		if env == "production" || env == "prod" {
			return fmt.Errorf("unregistered event source %s in production environment; cannot publish event", e.Source)
		}
		slog.Warn("Unregistered event source, executing inline (development fallback)", slog.String("source", e.Source))
		return r.Dispatch(ctx, e)
	}

	_, err := rc.Insert(ctx, args, nil)
	return err
}

func (r *EventRouter) PublishTx(ctx context.Context, tx *sql.Tx, e protocol.Event) error {
	r.mu.RLock()
	rc := r.riverClient
	r.mu.RUnlock()

	if rc == nil {
		env := os.Getenv("APP_ENV")
		if env == "production" || env == "prod" {
			return fmt.Errorf("database/River client is offline in production environment; cannot publish event")
		}
		slog.Warn("DB/River offline, executing subscribers inline (development fallback)", slog.String("source", e.Source))
		return r.Dispatch(ctx, e)
	}

	var args river.JobArgs
	switch e.Source {
	case "DISCOVERY":
		args = DiscoveryJobArgs{
			WorkspaceID: e.WorkspaceID,
			ID:          e.ID,
			Source:      e.Source,
			Target:      e.Target,
			Payload:     e.Payload,
			Timestamp:   e.Timestamp,
		}
	case "PREDATOR":
		args = PredatorJobArgs{
			WorkspaceID: e.WorkspaceID,
			ID:          e.ID,
			Source:      e.Source,
			Target:      e.Target,
			Payload:     e.Payload,
			Timestamp:   e.Timestamp,
		}
	case "SENTINEL_TEXT_OUTPUT":
		args = SentinelTextOutputJobArgs{
			WorkspaceID: e.WorkspaceID,
			ID:          e.ID,
			Source:      e.Source,
			Target:      e.Target,
			Payload:     e.Payload,
			Timestamp:   e.Timestamp,
		}
	case "MODELER_RESULT":
		args = ModelerResultJobArgs{
			WorkspaceID: e.WorkspaceID,
			ID:          e.ID,
			Source:      e.Source,
			Target:      e.Target,
			Payload:     e.Payload,
			Timestamp:   e.Timestamp,
		}
	default:
		env := os.Getenv("APP_ENV")
		if env == "production" || env == "prod" {
			return fmt.Errorf("unregistered event source %s in production environment; cannot publish event in tx", e.Source)
		}
		slog.Warn("Unregistered event source in tx, executing inline (development fallback)", slog.String("source", e.Source))
		return r.Dispatch(ctx, e)
	}

	_, err := rc.InsertTx(ctx, tx, args, nil)
	return err
}

// --- Job Args Structs ---

type DiscoveryJobArgs struct {
	WorkspaceID string `json:"workspace_id"`
	ID          string `json:"id"`
	Source      string `json:"source"`
	Target      string `json:"target"`
	Payload     string `json:"payload"`
	Timestamp   int64  `json:"timestamp"`
}

func (DiscoveryJobArgs) Kind() string { return "discovery" }
func (DiscoveryJobArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue:       "discovery",
		MaxAttempts: 5,
	}
}

type PredatorJobArgs struct {
	WorkspaceID string `json:"workspace_id"`
	ID          string `json:"id"`
	Source      string `json:"source"`
	Target      string `json:"target"`
	Payload     string `json:"payload"`
	Timestamp   int64  `json:"timestamp"`
}

func (PredatorJobArgs) Kind() string { return "predator" }
func (PredatorJobArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue:       "predator",
		MaxAttempts: 5,
	}
}

type SentinelTextOutputJobArgs struct {
	WorkspaceID string `json:"workspace_id"`
	ID          string `json:"id"`
	Source      string `json:"source"`
	Target      string `json:"target"`
	Payload     string `json:"payload"`
	Timestamp   int64  `json:"timestamp"`
}

func (SentinelTextOutputJobArgs) Kind() string { return "sentinel_text_output" }
func (SentinelTextOutputJobArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue:       "sentinel",
		MaxAttempts: 5,
	}
}

type ModelerResultJobArgs struct {
	WorkspaceID string `json:"workspace_id"`
	ID          string `json:"id"`
	Source      string `json:"source"`
	Target      string `json:"target"`
	Payload     string `json:"payload"`
	Timestamp   int64  `json:"timestamp"`
}

func (ModelerResultJobArgs) Kind() string { return "modeler_result" }
func (ModelerResultJobArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue:       "modeler",
		MaxAttempts: 5,
	}
}

type ProcessInvoiceJobArgs struct {
	JobID       string `json:"job_id"`
	WorkspaceID string `json:"workspace_id"`
	Payload     []byte `json:"payload"`
}

func (ProcessInvoiceJobArgs) Kind() string { return "process_invoice" }
func (ProcessInvoiceJobArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue: "modeler",
		UniqueOpts: river.UniqueOpts{
			ByArgs: true,
		},
		MaxAttempts: 3,
	}
}

type UpgradeWorkspaceJobArgs struct {
	WorkspaceID string `json:"workspace_id"`
	NewTier     string `json:"new_tier"`
	TokensToAdd int    `json:"tokens_to_add"`
	ReferenceID string `json:"reference_id"`
}

func (UpgradeWorkspaceJobArgs) Kind() string { return "upgrade_workspace" }
func (UpgradeWorkspaceJobArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue: "wallet",
		UniqueOpts: river.UniqueOpts{
			ByArgs: true,
		},
		MaxAttempts: 3,
	}
}

// --- River Workers ---

type DiscoveryWorker struct {
	river.WorkerDefaults[DiscoveryJobArgs]
	Router *EventRouter
}

func (w *DiscoveryWorker) Work(ctx context.Context, job *river.Job[DiscoveryJobArgs]) (err error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			slog.Error("Panic in DiscoveryWorker", slog.Any("panic", r), slog.Int64("job_id", job.ID))
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	event := protocol.Event{
		WorkspaceID: job.Args.WorkspaceID,
		ID:          job.Args.ID,
		Source:      job.Args.Source,
		Target:      job.Args.Target,
		Payload:     job.Args.Payload,
		Timestamp:   job.Args.Timestamp,
	}
	return w.Router.Dispatch(ctx, event)
}

type PredatorWorker struct {
	river.WorkerDefaults[PredatorJobArgs]
	Router *EventRouter
}

func (w *PredatorWorker) Work(ctx context.Context, job *river.Job[PredatorJobArgs]) (err error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			slog.Error("Panic in PredatorWorker", slog.Any("panic", r), slog.Int64("job_id", job.ID))
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	event := protocol.Event{
		WorkspaceID: job.Args.WorkspaceID,
		ID:          job.Args.ID,
		Source:      job.Args.Source,
		Target:      job.Args.Target,
		Payload:     job.Args.Payload,
		Timestamp:   job.Args.Timestamp,
	}
	return w.Router.Dispatch(ctx, event)
}

type SentinelTextOutputWorker struct {
	river.WorkerDefaults[SentinelTextOutputJobArgs]
	Router *EventRouter
}

func (w *SentinelTextOutputWorker) Work(ctx context.Context, job *river.Job[SentinelTextOutputJobArgs]) (err error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			slog.Error("Panic in SentinelTextOutputWorker", slog.Any("panic", r), slog.Int64("job_id", job.ID))
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	event := protocol.Event{
		WorkspaceID: job.Args.WorkspaceID,
		ID:          job.Args.ID,
		Source:      job.Args.Source,
		Target:      job.Args.Target,
		Payload:     job.Args.Payload,
		Timestamp:   job.Args.Timestamp,
	}
	return w.Router.Dispatch(ctx, event)
}

type ModelerResultWorker struct {
	river.WorkerDefaults[ModelerResultJobArgs]
	Router *EventRouter
}

func (w *ModelerResultWorker) Work(ctx context.Context, job *river.Job[ModelerResultJobArgs]) (err error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			slog.Error("Panic in ModelerResultWorker", slog.Any("panic", r), slog.Int64("job_id", job.ID))
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	event := protocol.Event{
		WorkspaceID: job.Args.WorkspaceID,
		ID:          job.Args.ID,
		Source:      job.Args.Source,
		Target:      job.Args.Target,
		Payload:     job.Args.Payload,
		Timestamp:   job.Args.Timestamp,
	}
	return w.Router.Dispatch(ctx, event)
}

type UpgradeWorkspaceWorker struct {
	river.WorkerDefaults[UpgradeWorkspaceJobArgs]
	DB *memory.RelationalStore
}

func (w *UpgradeWorkspaceWorker) Work(ctx context.Context, job *river.Job[UpgradeWorkspaceJobArgs]) (err error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			slog.Error("Panic in UpgradeWorkspaceWorker", slog.Any("panic", r), slog.Int64("job_id", job.ID))
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	// Thread workspace ID through context
	ctx = context.WithValue(ctx, memory.WorkspaceIDKey, job.Args.WorkspaceID)

	return w.DB.UpgradeWorkspaceTier(ctx, job.Args.WorkspaceID, job.Args.NewTier, job.Args.TokensToAdd, job.Args.ReferenceID)
}