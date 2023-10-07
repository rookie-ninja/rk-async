package rkasync

import (
	"context"
	"fmt"
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

	Stop(force bool, waitSec int)

	Database() Database
}

func NewLocalWorker(db Database, logger *rkentry.LoggerEntry, event *rkentry.EventEntry) *LocalWorker {
	return &LocalWorker{
		db:          db,
		lock:        sync.Mutex{},
		waitGroup:   sync.WaitGroup{},
		quitChannel: make(chan struct{}),
		logger:      logger,
		event:       event,
		startOnce:   sync.Once{},
		stopOnce:    sync.Once{},
	}
}

type LocalWorker struct {
	db          Database
	quitChannel chan struct{}
	waitGroup   sync.WaitGroup
	lock        sync.Mutex
	startOnce   sync.Once
	stopOnce    sync.Once

	logger *rkentry.LoggerEntry
	event  *rkentry.EventEntry
}

func (w *LocalWorker) Start() {
	w.startOnce.Do(func() {
		w.waitGroup.Add(1)

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
	})
}

func (w *LocalWorker) Stop(force bool, waitSec int) {
	w.stopOnce.Do(func() {
		if force {
			close(w.quitChannel)
			// wait for three seconds
			for i := 0; i < waitSec; i++ {
				w.logger.Info(fmt.Sprintf("shutting down worker, wait for %d second", i+1))
				time.Sleep(1 * time.Second)
			}
			return
		}

		w.logger.Info("shutting down worker, wait for jobs finish")
		close(w.quitChannel)
		w.waitGroup.Wait()
	})
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

	event := w.event.Start("processJob",
		rkquery.WithEntryName(job.Type),
		rkquery.WithEntryType(job.Id))
	defer func() {
		event.SetEndTime(time.Now())
		event.Finish()
	}()
	event.SetResCode("OK")

	logger := w.logger.With(zap.String("id", job.Id),
		zap.String("type", job.Type),
		zap.String("userId", job.UserId))

	logger.Info("pick job to work")
	defer logger.Info("finished job")

	// process job & record error
	ctx := context.WithValue(context.Background(), LoggerKey, logger)
	ctx = context.WithValue(ctx, EventKey, event)

	processor := w.Database().GetProcessor(job.Type)
	if processor == nil {
		logger.Warn("processor is nil, aborting...")
		job.State = JobStateFailed
		if err := w.db.UpdateJob(job); err != nil {
			logger.Warn("failed to update job state", zap.Error(err))
		}
		return
	}

	err = processor.Process(ctx, job, w.Database().UpdateJob)
	event.AddPair("jobType", job.Type)
	event.AddPair("jobId", job.Id)

	if err != nil {
		event.AddErr(err)
		event.SetResCode("Fail")

		logger.Error("failed to process job", zap.Error(err))

		job.State = JobStateFailed
		if err := w.db.UpdateJob(job); err != nil {
			logger.Error("failed to update job state",
				zap.String("state", JobStateFailed),
				zap.Error(err))
		}
		return
	}

	// update DB
	job.State = JobStateSuccess
	if err := w.db.UpdateJob(job); err != nil {
		logger.Error("failed to update job state", zap.String("state", JobStateSuccess))
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
