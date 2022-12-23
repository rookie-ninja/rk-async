package rkasync

import (
	"context"
	"github.com/rookie-ninja/rk-entry/v2/entry"
	rkquery "github.com/rookie-ninja/rk-query"
	"go.uber.org/zap"
	"sync"
	"time"
)

const (
	LoggerKey = "rk-async-logger"
	EventKey  = "rk-async-event"
)

type Worker interface {
	Start()

	Stop()

	Database() Database
}

func NewLocalWorker(db Database, logger *rkentry.LoggerEntry, event *rkentry.EventEntry) *LocalWorker {
	return &LocalWorker{
		db:          db,
		lock:        sync.Mutex{},
		once:        sync.Once{},
		waitGroup:   sync.WaitGroup{},
		quitChannel: make(chan struct{}),
		logger:      logger,
		event:       event,
	}
}

type LocalWorker struct {
	db          Database
	quitChannel chan struct{}
	waitGroup   sync.WaitGroup
	lock        sync.Mutex
	once        sync.Once

	logger *rkentry.LoggerEntry
	event  *rkentry.EventEntry
}

func (w *LocalWorker) Start() {
	w.once.Do(func() {
		w.waitGroup.Add(1)
	})

	go func() {
		waitChannel := time.NewTimer(time.Duration(1) * time.Second)

		defer func() {
			w.waitGroup.Done()
		}()

		for {
			select {
			case <-w.quitChannel:
				return
			case <-waitChannel.C:
				w.processJob()
				waitChannel.Reset(time.Duration(1) * time.Second)
			default:
				w.processJob()
				time.Sleep(time.Duration(1) * time.Second)
			}
		}
	}()
}

func (w *LocalWorker) Stop() {
	close(w.quitChannel)
	w.waitGroup.Wait()
}

func (w *LocalWorker) processJob() {
	// pick a job
	job, err := w.db.PickJobToWork()
	if err != nil {
		w.logger.Error("failed to pick job", zap.Error(err))
		return
	}
	if job == nil {
		return
	}

	event := w.event.Start("processJob")
	defer event.Finish()
	event.SetResCode("OK")

	logger := w.logger.With(zap.String("id", job.Meta().Id),
		zap.String("type", job.Meta().Type),
		zap.String("user", job.Meta().User),
		zap.String("class", job.Meta().Class),
		zap.String("category", job.Meta().Category))

	// process job & record error
	ctx := context.WithValue(context.Background(), LoggerKey, logger)
	ctx = context.WithValue(ctx, EventKey, event)

	err = job.Process(ctx)
	event.AddPair("jobType", job.Meta().Type)
	event.AddPair("jobId", job.Meta().Id)

	if err != nil {
		job.Meta().Error = err.Error()
		event.AddErr(err)
		event.SetResCode("Fail")

		w.logger.Error("failed to process job",
			zap.String("id", job.Meta().Id),
			zap.String("type", job.Meta().Type),
			zap.String("user", job.Meta().User),
			zap.String("class", job.Meta().Class),
			zap.String("category", job.Meta().Category))

		if err := w.db.UpdateJobState(job, JobStateFailed); err != nil {
			w.logger.Error("failed to update job state",
				zap.String("id", job.Meta().Id),
				zap.String("type", job.Meta().Type),
				zap.String("user", job.Meta().User),
				zap.String("class", job.Meta().Class),
				zap.String("category", job.Meta().Category),
				zap.String("state", JobStateFailed))
			return
		}
	}

	// update DB
	if err := w.db.UpdateJobState(job, JobStateSuccess); err != nil {
		w.logger.Error("failed to update job state",
			zap.String("id", job.Meta().Id),
			zap.String("type", job.Meta().Type),
			zap.String("user", job.Meta().User),
			zap.String("class", job.Meta().Class),
			zap.String("category", job.Meta().Category),
			zap.String("state", JobStateSuccess))
	}
}

func (w *LocalWorker) Database() Database {
	return w.db
}

func GetLoggerFromCtx(ctx context.Context) *zap.Logger {
	raw := ctx.Value(LoggerKey)
	if raw == nil {
		return rkentry.GlobalAppCtx.GetLoggerEntryDefault().Logger
	}

	if v, ok := raw.(*zap.Logger); ok {
		return v
	}

	return rkentry.GlobalAppCtx.GetLoggerEntryDefault().Logger
}

func GetEventFromCtx(ctx context.Context) rkquery.Event {
	raw := ctx.Value(EventKey)
	if raw == nil {
		return rkentry.GlobalAppCtx.GetEventEntryDefault().CreateEvent()
	}

	if v, ok := raw.(rkquery.Event); ok {
		return v
	}

	return rkentry.GlobalAppCtx.GetEventEntryDefault().CreateEvent()
}
