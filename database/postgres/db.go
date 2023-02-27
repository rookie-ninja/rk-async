package postgres

import (
	"fmt"
	"github.com/rookie-ninja/rk-async"
	"github.com/rookie-ninja/rk-db/postgres"
	"github.com/rs/xid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"sync"
	"time"
)

func init() {
	rkasync.RegisterDatabaseRegFunc("PostgreSQL", RegisterDatabase)
}

func RegisterDatabase(m map[string]string) rkasync.Database {
	entry := rkpostgres.GetPostgresEntry(m["entryName"])
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

	return &Database{
		db:         db,
		lock:       &sync.Mutex{},
		processorM: map[string]rkasync.Processor{},
	}
}

type Database struct {
	db         *gorm.DB
	lock       sync.Locker
	processorM map[string]rkasync.Processor
}

func (e *Database) RegisterProcessor(jobType string, processor rkasync.Processor) {
	e.processorM[jobType] = processor
}

func (e *Database) GetProcessor(jobType string) rkasync.Processor {
	return e.processorM[jobType]
}

func (e *Database) Type() string {
	return "PostgreSQL"
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

func (e *Database) PickJobToWork() (*rkasync.Job, error) {
	var job *rkasync.Job
	err := e.db.Transaction(func(tx *gorm.DB) error {
		res := &rkasync.Job{}
		// get job with state created
		resDB := tx.Where("state = ?", rkasync.JobStateCreated).Limit(1).Find(res)

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

func (e *Database) UpdateJobState(job *rkasync.Job, state string) error {
	err := e.db.Transaction(func(tx *gorm.DB) error {
		resDB := tx.Updates(job)
		if resDB.Error != nil {
			return resDB.Error
		}
		if resDB.RowsAffected < 1 {
			return fmt.Errorf("failed to update job state, no rows updated, id:%s, state:%s", job.Id, state)
		}

		return nil
	})
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
