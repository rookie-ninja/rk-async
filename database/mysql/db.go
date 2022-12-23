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
	async.RegisterDatabaseRegFunc("MySQL", RegisterDatabase)
}

func RegisterDatabase(m map[string]string) async.Database {
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
		db:         db,
		marshalF:   map[string]func(async.Job) ([]byte, error){},
		unmarshalF: map[string]func([]byte) (async.Job, error){},
	}
}

type Database struct {
	db         *gorm.DB
	lock       sync.Locker
	marshalF   map[string]func(async.Job) ([]byte, error)
	unmarshalF map[string]func([]byte) (async.Job, error)
}

func (e *Database) Type() string {
	return "MySQL"
}

func (e *Database) AddJob(job async.Job) error {
	if job.GetMeta() == nil {
		return fmt.Errorf("nil job meta")
	}

	job.GetMeta().Id = xid.New().String()
	job.GetMeta().State = async.JobStateCreated

	wrapper := &Wrapper{
		JobMeta: job.GetMeta(),
	}

	mar := e.getMarshaller(job.GetMeta().Type)
	if mar == nil {
		return fmt.Errorf("nil job marshaller")
	}

	b, err := mar(job)
	if err != nil {
		return err
	}
	wrapper.JobRaw = string(b)

	// sync to DB
	resDB := e.db.Create(wrapper)

	return resDB.Error
}

func (e *Database) PickJobToWork() (async.Job, error) {
	var job async.Job
	err := e.db.Transaction(func(tx *gorm.DB) error {
		// get job with state created
		createdJob := &Wrapper{}
		resDB := tx.Where("state = ?", async.JobStateCreated).Limit(1).Find(createdJob)

		// update state for job structure
		unmar := e.getUnmarshaler(createdJob.Type)
		if unmar == nil {
			return fmt.Errorf("nil job unmarshaler")
		}

		unMarJob, err := unmar([]byte(createdJob.JobRaw))
		if err != nil {
			return err
		}

		unMarJob.GetMeta().State = async.JobStateRunning
		unMarJob.GetMeta().UpdatedAt = time.Now()

		// update in DB
		createdJob.JobMeta = unMarJob.GetMeta()
		mar := e.getMarshaller(createdJob.Type)
		b, err := mar(unMarJob)
		if err != nil {
			return err
		}
		createdJob.JobRaw = string(b)

		resDB = tx.Updates(createdJob)
		if resDB.Error != nil {
			return resDB.Error
		}
		if resDB.RowsAffected < 1 {
			return fmt.Errorf("failed to update job state, id:%s, state:%s", createdJob.Id, async.JobStateRunning)
		}

		job = unMarJob

		return nil
	})

	return job, err
}

func (e *Database) UpdateJobState(job async.Job, state string) error {
	err := e.db.Transaction(func(tx *gorm.DB) error {
		oldJob := &Wrapper{}

		resDB := tx.Where("id = ?", job.GetMeta().Id).Find(oldJob)
		if resDB.Error != nil {
			return resDB.Error
		}

		if resDB.RowsAffected < 1 {
			return fmt.Errorf("job with %s not found", job.GetMeta().Id)
		}

		if !async.JobNewStateAllowed(oldJob.State, state) {
			return fmt.Errorf("job state mutation not allowed by policy, %s->%s", oldJob.State, state)
		}

		// update state for job structure
		unmar := e.getUnmarshaler(oldJob.Type)
		if unmar == nil {
			return fmt.Errorf("nil job unmarshaler")
		}

		unMarJob, err := unmar([]byte(oldJob.JobRaw))
		if err != nil {
			return err
		}

		unMarJob.GetMeta().State = state
		unMarJob.GetMeta().UpdatedAt = time.Now()

		// update in DB
		oldJob.JobMeta = unMarJob.GetMeta()
		mar := e.getMarshaller(oldJob.Type)
		b, err := mar(unMarJob)
		if err != nil {
			return err
		}
		oldJob.JobRaw = string(b)

		resDB = tx.Updates(oldJob)
		if resDB.Error != nil {
			return resDB.Error
		}
		if resDB.RowsAffected < 1 {
			return fmt.Errorf("failed to update job state, id:%s, state:%s", job.GetMeta().Id, state)
		}

		return nil
	})
	return err
}

// TODO: Paginator

func (e *Database) ListJobs(filter *async.JobFilter) ([]async.Job, error) {
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

	res := make([]async.Job, 0)

	for i := range jobList {
		wrap := jobList[i]

		unmar := e.getUnmarshaler(wrap.Type)
		if unmar == nil {
			return nil, fmt.Errorf("nil job unmarshaler")
		}

		unMarJob, err := unmar([]byte(wrap.JobRaw))
		if err != nil {
			return nil, err
		}

		unMarJob.GetMeta().State = wrap.State
		unMarJob.GetMeta().UpdatedAt = wrap.UpdatedAt

		res = append(res, unMarJob)
	}

	return res, nil
}

func (e *Database) GetJob(id string) (async.Job, error) {
	wrap := &Wrapper{}

	resDB := e.db.Where("id = ?", id).Find(&wrap)
	if resDB.Error != nil {
		return nil, resDB.Error
	}
	if resDB.RowsAffected < 1 {
		return nil, fmt.Errorf("job not found with id=%s", id)
	}

	unmar := e.getUnmarshaler(wrap.Type)
	if unmar == nil {
		return nil, fmt.Errorf("nil job unmarshaler")
	}

	unMarJob, err := unmar([]byte(wrap.JobRaw))
	if err != nil {
		return nil, err
	}

	unMarJob.GetMeta().State = wrap.State
	unMarJob.GetMeta().UpdatedAt = wrap.UpdatedAt

	return unMarJob, nil
}

func (e *Database) CancelJobsOverdue(days int) error {
	err := e.db.Transaction(func(tx *gorm.DB) error {
		due := time.Now().AddDate(0, 0, -days)

		resDB := tx.Model(&Wrapper{}).Where("state = ? AND updated_at < ?",
			async.JobStateRunning, due).Update("state", async.JobStateCanceled)
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
			async.JobStateFailed, async.JobStateSuccess, async.JobStateCanceled,
		}

		resDB := tx.Model(&Wrapper{}).Delete("state IN ? AND updated_at < ?", states, due)
		if resDB.Error != nil {
			return resDB.Error
		}

		return nil
	})

	return err
}

func (e *Database) RegisterMarshaller(jobType string, f func(async.Job) ([]byte, error)) {
	e.lock.Lock()
	defer e.lock.Unlock()

	e.marshalF[jobType] = f
}

func (e *Database) RegisterUnmarshaler(jobType string, f func([]byte) (async.Job, error)) {
	e.lock.Lock()
	defer e.lock.Unlock()

	e.unmarshalF[jobType] = f
}

func (e *Database) getMarshaller(jobType string) func(async.Job) ([]byte, error) {
	e.lock.Lock()
	defer e.lock.Unlock()

	return e.marshalF[jobType]
}

func (e *Database) getUnmarshaler(jobType string) func([]byte) (async.Job, error) {
	e.lock.Lock()
	defer e.lock.Unlock()

	return e.unmarshalF[jobType]
}

type Wrapper struct {
	*async.JobMeta
	JobRaw string `json:"-" yaml:"-" gorm:"longtext"`
}

func (w *Wrapper) TableName() string {
	return "async_job"
}
