package mysql

import (
	"errors"
	"fmt"
	"github.com/rookie-ninja/rk-async"
	rkmysql "github.com/rookie-ninja/rk-db/mysql"
	"github.com/rs/xid"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"sync"
	"time"
)

func init() {
	rkasync.RegisterDatabaseRegFunc("MySQL", RegisterDatabase)
}

func RegisterDatabase(m map[string]string, logger *zap.Logger) rkasync.Database {
	entry := rkmysql.GetMySqlEntry(m["entryName"])
	if entry == nil {
		return nil
	}

	db := entry.GetDB(m["database"])
	if db == nil {
		return nil
	}

	if !db.DryRun {
		db.AutoMigrate(&rkasync.Job{})
	}

	if logger == nil {
		logger = zap.NewNop()
	}

	return &Database{
		db:         db,
		logger:     logger,
		processorM: map[string]rkasync.Processor{},
		lock:       &sync.Mutex{},
	}
}

type Database struct {
	db         *gorm.DB
	logger     *zap.Logger
	lock       sync.Locker
	processorM map[string]rkasync.Processor
}

func (e *Database) Type() string {
	return "MySQL"
}

func (e *Database) RegisterProcessor(jobType string, processor rkasync.Processor) {
	e.processorM[jobType] = processor
}

func (e *Database) GetProcessor(jobType string) rkasync.Processor {
	return e.processorM[jobType]
}

func (e *Database) AddJob(job *rkasync.Job) error {
	if job == nil {
		return fmt.Errorf("nil job")
	}

	if len(job.Id) < 1 {
		job.Id = xid.New().String()
	}
	job.State = rkasync.JobStateCreated

	// sync to DB
	resDB := e.db.Create(job)

	return resDB.Error
}

func (e *Database) PickJobToWorkWithId(jobId string) (*rkasync.Job, error) {
	var job *rkasync.Job
	err := e.db.Transaction(func(tx *gorm.DB) error {
		res := &rkasync.Job{}
		// get job with state created
		resDB := tx.Where("state = ? AND id = ?", rkasync.JobStateCreated, jobId).Limit(1).Find(res)

		if resDB.Error != nil {
			return resDB.Error
		}

		if resDB.RowsAffected < 1 {
			return errors.New("job not found")
		}

		res.State = rkasync.JobStateRunning
		res.UpdatedAt = time.Now()

		resDB = tx.Updates(res)
		if resDB.Error != nil {
			return resDB.Error
		}
		if resDB.RowsAffected < 1 {
			return fmt.Errorf("failed to update job state, id:%s, state:%s", job.Id, rkasync.JobStateRunning)
		}

		job = res

		return nil
	})

	return job, err
}

func (e *Database) PickJobToWork() (*rkasync.Job, error) {
	var job *rkasync.Job
	err := e.db.Transaction(func(tx *gorm.DB) error {
		res := &rkasync.Job{}
		// get job with state created
		resDB := tx.Where("state = ?", rkasync.JobStateCreated).Order("created_at ASC").Limit(1).Find(res)

		if resDB.Error != nil {
			return resDB.Error
		}

		if resDB.RowsAffected < 1 {
			return nil
		}

		res.State = rkasync.JobStateRunning
		res.UpdatedAt = time.Now()

		resDB = tx.Updates(res)
		if resDB.Error != nil {
			return resDB.Error
		}
		if resDB.RowsAffected < 1 {
			return fmt.Errorf("failed to update job state, id:%s, state:%s", job.Id, rkasync.JobStateRunning)
		}

		job = res

		return nil
	})

	return job, err
}

func (e *Database) UpdateJobPayloadAndStep(job *rkasync.Job) error {
	var err error
	// retry
	for retry := 0; retry < 3; retry++ {
		err = e.db.Transaction(func(tx *gorm.DB) error {
			if job == nil {
				return nil
			}

			jobDB := &rkasync.Job{}
			resDB := tx.Where("id = ?", job.Id).Find(jobDB)
			if resDB.Error != nil {
				return resDB.Error
			}
			if resDB.RowsAffected < 1 {
				return fmt.Errorf("job not found, id:%s", job.Id)
			}

			if jobDB.State == "canceled" || jobDB.State == "success" || jobDB.State == "failed" {
				e.logger.Warn(fmt.Sprintf("job at final state:%s, skip updating payloads and steps", jobDB.State))
				return nil
			}

			resDB = tx.Model(&rkasync.Job{}).Where("id = ?", job.Id).UpdateColumns(
				rkasync.Job{
					UpdatedAt: time.Now(),
					Payload:   job.Payload,
					Steps:     job.Steps,
				})

			if resDB.Error != nil {
				return resDB.Error
			}
			if resDB.RowsAffected < 1 {
				return fmt.Errorf("failed to update job payloads and steps, no rows updated, id:%s", job.Id)
			}

			return nil
		})

		if err == nil {
			return nil
		}
	}

	e.logger.Warn("failed to update job payloads and steps", zap.Error(err))

	return err
}

func (e *Database) UpdateJobState(job *rkasync.Job) error {
	var err error
	// retry
	for retry := 0; retry < 3; retry++ {
		err = e.db.Transaction(func(tx *gorm.DB) error {
			if job == nil {
				return nil
			}

			jobDB := &rkasync.Job{}
			resDB := tx.Where("id = ?", job.Id).Find(jobDB)
			if resDB.Error != nil {
				return resDB.Error
			}
			if resDB.RowsAffected < 1 {
				return fmt.Errorf("job not found, id:%s", job.Id)
			}

			if jobDB.State == "canceled" || jobDB.State == "success" || jobDB.State == "failed" {
				e.logger.Warn(fmt.Sprintf("job at final state:%s, skip updating state %s", jobDB.State, job.State))
				return nil
			}

			resDB = tx.Model(&rkasync.Job{}).Where("id = ?", job.Id).UpdateColumns(
				rkasync.Job{
					UpdatedAt: time.Now(),
					State:     job.State,
				})

			if resDB.Error != nil {
				return resDB.Error
			}
			if resDB.RowsAffected < 1 {
				return fmt.Errorf("failed to update job state, no rows updated, id:%s, state:%s", job.Id, job.State)
			}

			return nil
		})

		if err == nil {
			return nil
		}
	}

	e.logger.Warn("failed to update job state", zap.Error(err))

	return err
}

// TODO: Paginator

func (e *Database) ListJobs(filter *rkasync.JobFilter) ([]*rkasync.Job, error) {
	clauses := make([]clause.Expression, 0)

	limit := 100
	order := "updated_at desc"

	if filter != nil {
		if filter.Limit > 0 {
			limit = filter.Limit
		}

		if filter.ClauseList != nil {
			clauses = append(clauses, filter.ClauseList...)
		}

		if len(filter.Order) > 0 {
			order = filter.Order
		}
	}

	jobList := make([]*rkasync.Job, 0)

	resDB := e.db.Clauses(clauses...).Distinct().Limit(limit).Order(order).Find(&jobList)
	if resDB.Error != nil {
		return nil, resDB.Error
	}

	return jobList, nil
}

func (e *Database) GetJob(id string) (*rkasync.Job, error) {
	job := &rkasync.Job{}

	resDB := e.db.Where("id = ?", id).Find(job)
	if resDB.Error != nil {
		return nil, resDB.Error
	}
	if resDB.RowsAffected < 1 {
		return nil, fmt.Errorf("job not found with id=%s", id)
	}

	return job, nil
}

func (e *Database) CancelJobsOverdue(days int, filter *rkasync.JobFilter) error {
	clauses := make([]clause.Expression, 0)

	if filter != nil && filter.ClauseList != nil {
		clauses = append(clauses, filter.ClauseList...)
	}

	err := e.db.Transaction(func(tx *gorm.DB) error {
		due := time.Now().AddDate(0, 0, -days)

		resDB := tx.Model(&rkasync.Job{}).Clauses(clauses...).Where("state = ? AND updated_at < ?",
			rkasync.JobStateRunning, due).Update("state", rkasync.JobStateCanceled)
		if resDB.Error != nil {
			return resDB.Error
		}

		return nil
	})

	return err
}

func (e *Database) CleanJobs(days int, filter *rkasync.JobFilter) error {
	clauses := make([]clause.Expression, 0)

	if filter != nil && filter.ClauseList != nil {
		clauses = append(clauses, filter.ClauseList...)
	}

	err := e.db.Transaction(func(tx *gorm.DB) error {
		due := time.Now().AddDate(0, 0, -days)

		states := []string{
			rkasync.JobStateFailed, rkasync.JobStateSuccess, rkasync.JobStateCanceled,
		}

		resDB := tx.Clauses(clauses...).Where("state IN ? AND updated_at < ?", states, due).Delete(&rkasync.Job{})
		if resDB.Error != nil {
			return resDB.Error
		}

		return nil
	})

	return err
}
