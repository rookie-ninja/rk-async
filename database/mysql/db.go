package mysql

import (
	"fmt"
	"github.com/rookie-ninja/rk-async"
	rkmysql "github.com/rookie-ninja/rk-db/mysql"
	"github.com/rs/xid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"sync"
	"time"
)

func init() {
	rkasync.RegisterDatabaseRegFunc("MySQL", RegisterDatabase)
}

func RegisterDatabase(m map[string]string) rkasync.Database {
	entry := rkmysql.GetMySqlEntry(m["entryName"])
	if entry == nil {
		return nil
	}

	db := entry.GetDB(m["database"])
	if db == nil {
		return nil
	}

	if !db.DryRun {
		db.AutoMigrate(&Wrapper{})
	}

	return &Database{
		db:           db,
		unmarshalerM: map[string]rkasync.UnmarshalerFunc{},
		lock:         &sync.Mutex{},
	}
}

type Database struct {
	db           *gorm.DB
	lock         sync.Locker
	unmarshalerM map[string]rkasync.UnmarshalerFunc
}

func (e *Database) Type() string {
	return "MySQL"
}

func (e *Database) RegisterJob(job rkasync.Job) {
	e.lock.Lock()
	defer e.lock.Unlock()

	e.unmarshalerM[job.Meta().Type] = job.Unmarshal
}

func (e *Database) UnmarshalJob(b []byte, meta *rkasync.JobMeta) (rkasync.Job, error) {
	e.lock.Lock()
	defer e.lock.Unlock()

	unmar, ok := e.unmarshalerM[meta.Type]
	if !ok {
		return nil, fmt.Errorf("unsupported job type %s, please register job first", meta.Type)
	}

	return unmar(b, meta)
}

func (e *Database) AddJob(job rkasync.Job) error {
	if job.Meta() == nil {
		return fmt.Errorf("nil job meta")
	}

	job.Meta().Id = xid.New().String()
	job.Meta().State = rkasync.JobStateCreated

	wrapper := &Wrapper{
		JobMeta: job.Meta(),
	}

	b, err := job.Marshal()
	if err != nil {
		return err
	}
	wrapper.JobRaw = string(b)

	// sync to DB
	resDB := e.db.Create(wrapper)

	return resDB.Error
}

func (e *Database) PickJobToWork() (rkasync.Job, error) {
	var res rkasync.Job
	err := e.db.Transaction(func(tx *gorm.DB) error {
		// get job with state created
		wrap := &Wrapper{}
		resDB := tx.Where("state = ?", rkasync.JobStateCreated).Limit(1).Find(wrap)

		if resDB.Error != nil {
			return resDB.Error
		}

		if resDB.RowsAffected < 1 {
			return nil
		}

		wrap.JobMeta.State = rkasync.JobStateRunning
		wrap.JobMeta.UpdatedAt = time.Now()

		// update state for job structure
		job, err := e.UnmarshalJob([]byte(wrap.JobRaw), wrap.JobMeta)
		if err != nil {
			return err
		}

		// update in DB
		b, err := job.Marshal()
		if err != nil {
			return err
		}
		wrap.JobRaw = string(b)

		resDB = tx.Updates(wrap)
		if resDB.Error != nil {
			return resDB.Error
		}
		if resDB.RowsAffected < 1 {
			return fmt.Errorf("failed to update job state, id:%s, state:%s", wrap.Id, rkasync.JobStateRunning)
		}

		res = job

		return nil
	})

	return res, err
}

func (e *Database) UpdateJobState(job rkasync.Job, state string) error {
	err := e.db.Transaction(func(tx *gorm.DB) error {
		if !rkasync.JobNewStateAllowed(job.Meta().State, state) {
			return fmt.Errorf("job state mutation not allowed by policy, %s->%s", job.Meta().State, state)
		}

		job.Meta().State = state

		wrap := &Wrapper{
			JobMeta: job.Meta(),
		}

		// update in DB
		b, err := job.Marshal()
		if err != nil {
			return err
		}
		wrap.JobRaw = string(b)

		resDB := tx.Updates(wrap)
		if resDB.Error != nil {
			return resDB.Error
		}
		if resDB.RowsAffected < 1 {
			return fmt.Errorf("failed to update job state, id:%s, state:%s", job.Meta().Id, state)
		}

		return nil
	})
	return err
}

// TODO: Paginator

func (e *Database) ListJobs(filter *rkasync.JobFilter) ([]rkasync.Job, error) {
	clauses := make([]clause.Expression, 0)

	if filter != nil {
		for i := range filter.TypeList {
			clauses = append(clauses, clause.Eq{
				Column: "type",
				Value:  filter.TypeList[i],
			})
		}

		for i := range filter.UserList {
			clauses = append(clauses, clause.Eq{
				Column: "user",
				Value:  filter.UserList[i],
			})
		}

		for i := range filter.ClassList {
			clauses = append(clauses, clause.Eq{
				Column: "class",
				Value:  filter.ClassList[i],
			})
		}

		for i := range filter.CategoryList {
			clauses = append(clauses, clause.Eq{
				Column: "category",
				Value:  filter.CategoryList[i],
			})
		}
	}

	jobList := make([]*Wrapper, 0)

	resDB := e.db.Clauses(clauses...).Distinct().Find(&jobList)
	if resDB.Error != nil {
		return nil, resDB.Error
	}

	res := make([]rkasync.Job, 0)

	for i := range jobList {
		wrap := jobList[i]

		job, err := e.UnmarshalJob([]byte(wrap.JobRaw), wrap.JobMeta)
		if err != nil {
			continue
		}

		res = append(res, job)
	}

	return res, nil
}

func (e *Database) GetJob(id string) (rkasync.Job, error) {
	wrap := &Wrapper{}

	resDB := e.db.Where("id = ?", id).Find(&wrap)
	if resDB.Error != nil {
		return nil, resDB.Error
	}
	if resDB.RowsAffected < 1 {
		return nil, fmt.Errorf("job not found with id=%s", id)
	}

	return e.UnmarshalJob([]byte(wrap.JobRaw), wrap.JobMeta)
}

func (e *Database) CancelJobsOverdue(days int) error {
	err := e.db.Transaction(func(tx *gorm.DB) error {
		due := time.Now().AddDate(0, 0, -days)

		resDB := tx.Model(&Wrapper{}).Where("state = ? AND updated_at < ?",
			rkasync.JobStateRunning, due).Update("state", rkasync.JobStateCanceled)
		if resDB.Error != nil {
			return resDB.Error
		}

		return nil
	})

	return err
}

func (e *Database) CleanJobs(days int) error {
	err := e.db.Transaction(func(tx *gorm.DB) error {
		due := time.Now().AddDate(0, 0, -days)

		states := []string{
			rkasync.JobStateFailed, rkasync.JobStateSuccess, rkasync.JobStateCanceled,
		}

		resDB := tx.Where("state IN ? AND updated_at < ?", states, due).Delete(&Wrapper{})
		if resDB.Error != nil {
			return resDB.Error
		}

		return nil
	})

	return err
}

type Wrapper struct {
	*rkasync.JobMeta
	JobRaw string `json:"-" yaml:"-" gorm:"longtext"`
}

func (w *Wrapper) TableName() string {
	return "async_job"
}
