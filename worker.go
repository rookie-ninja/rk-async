package async

import (
	"context"
	"github.com/rookie-ninja/rk-entry/v2/entry"
	"go.uber.org/zap"
	"sync"
	"time"
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

	// process job & record error
	err = job.Start(context.Background())
	event.AddPair("jobType", job.Type())
	event.AddPair("jobId", job.GetMeta().Id)

	if err != nil {
		job.RecordError(err)
		event.AddErr(err)
		event.SetResCode("Fail")

		w.logger.Error("failed to process job",
			zap.String("id", job.GetMeta().Id),
			zap.String("type", job.GetMeta().Type),
			zap.String("user", job.GetMeta().User),
			zap.String("class", job.GetMeta().Class),
			zap.String("category", job.GetMeta().Category))

		if err := w.db.UpdateJobState(job, JobStateFailed); err != nil {
			w.logger.Error("failed to update job state",
				zap.String("id", job.GetMeta().Id),
				zap.String("type", job.GetMeta().Type),
				zap.String("user", job.GetMeta().User),
				zap.String("class", job.GetMeta().Class),
				zap.String("category", job.GetMeta().Category),
				zap.String("state", JobStateFailed))
			return
		}
	}

	// update DB
	if err := w.db.UpdateJobState(job, JobStateSuccess); err != nil {
		w.logger.Error("failed to update job state",
			zap.String("id", job.GetMeta().Id),
			zap.String("type", job.GetMeta().Type),
			zap.String("user", job.GetMeta().User),
			zap.String("class", job.GetMeta().Class),
			zap.String("category", job.GetMeta().Category),
			zap.String("state", JobStateSuccess))
	}
}

func (w *LocalWorker) Database() Database {
	return w.db
}
