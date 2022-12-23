package rkasync

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/rookie-ninja/rk-entry/v2/entry"
)

func init() {
	rkentry.RegisterUserEntryRegFunc(RegisterEntriesFromConfig)
}

var (
	dbRegFuncM = map[string]func(map[string]string) Database{}
)

func GetEntry() *Entry {
	res := rkentry.GlobalAppCtx.GetEntry("RkAsyncEntry", "rk-async-entry")
	if res == nil {
		return nil
	}

	if v, ok := res.(*Entry); ok {
		return v
	}

	return nil
}

func RegisterDatabaseRegFunc(dbType string, f func(map[string]string) Database) {
	dbRegFuncM[dbType] = f
}

func RegisterEntriesFromConfig(raw []byte) map[string]rkentry.Entry {
	res := make(map[string]rkentry.Entry)

	// 1: decode config map into boot config struct
	config := &BootConfig{}
	rkentry.UnmarshalBootYAML(raw, config)

	// 3: construct entry
	if config.Async.Enabled {
		entry := &Entry{
			config: config,
		}
		res[entry.GetName()] = entry
		rkentry.GlobalAppCtx.AddEntry(entry)
	}
	return res
}

type BootConfig struct {
	Async struct {
		Enabled  bool   `json:"enabled" yaml:"enabled"`
		Logger   string `json:"logger" yaml:"logger"`
		Event    string `json:"event" yaml:"event"`
		Database struct {
			MySql struct {
				Enabled   bool   `json:"enabled" yaml:"enabled"`
				EntryName string `json:"entryName" yaml:"entryName"`
				Database  string `json:"database" yaml:"database"`
			} `yaml:"mySql" json:"mySql"`
		} `yaml:"database" json:"database"`
		Worker struct {
			Local struct {
				Enabled bool `json:"enabled" yaml:"enabled"`
			} `yaml:"local" json:"local"`
		} `yaml:"worker" json:"worker"`
	} `yaml:"async" json:"async"`
}

type Entry struct {
	db     Database
	config *BootConfig
	worker Worker
}

func (e *Entry) Bootstrap(ctx context.Context) {
	var db Database
	if e.config.Async.Database.MySql.Enabled {
		f := dbRegFuncM["MySQL"]
		db = f(map[string]string{
			"entryName": e.config.Async.Database.MySql.EntryName,
			"database":  e.config.Async.Database.MySql.Database,
		})
	}

	if db == nil {
		rkentry.ShutdownWithError(errors.New("db is nil"))
	}

	e.db = db

	// logger
	logger := rkentry.GlobalAppCtx.GetLoggerEntry(e.config.Async.Logger)
	if logger == nil {
		logger = rkentry.GlobalAppCtx.GetLoggerEntryDefault()
	}

	// event
	event := rkentry.GlobalAppCtx.GetEventEntry(e.config.Async.Event)
	if event == nil {
		event = rkentry.GlobalAppCtx.GetEventEntryDefault()
	}

	// worker
	if e.config.Async.Worker.Local.Enabled {
		e.worker = NewLocalWorker(db, logger, event)
	}
}

func (e *Entry) Interrupt(ctx context.Context) {
	e.worker.Stop()
}

func (e *Entry) GetName() string {
	return "rk-async-entry"
}

func (e *Entry) GetType() string {
	return "RkAsyncEntry"
}

func (e *Entry) GetDescription() string {
	return "async job entry"
}

func (e *Entry) String() string {
	m := map[string]interface{}{
		"dbType": e.db.Type(),
	}

	b, _ := json.Marshal(m)
	return string(b)
}

func (e *Entry) StartWorker() {
	if e.worker != nil {
		e.worker.Start()
	}
}

func (e *Entry) StopWorker() {
	if e.worker != nil {
		e.worker.Stop()
	}
}

func (e *Entry) Worker() Worker {
	return e.worker
}

func (e *Entry) Database() Database {
	return e.db
}

func (e *Entry) AddJob(job Job) error {
	return e.db.AddJob(job)
}

func (e *Entry) UpdateJobState(job Job, state string) error {
	return e.db.UpdateJobState(job, state)
}

func (e *Entry) ListJobs(filter *JobFilter) ([]Job, error) {
	return e.db.ListJobs(filter)
}

func (e *Entry) GetJob(id string) (Job, error) {
	return e.db.GetJob(id)
}

func (e *Entry) CancelJobsOverdue(days int, filter *JobFilter) error {
	return e.db.CancelJobsOverdue(days, filter)
}

func (e *Entry) CleanJobs(days int, filter *JobFilter) error {
	return e.db.CleanJobs(days, filter)
}

func (e *Entry) RegisterJob(job Job) {
	e.db.RegisterJob(job)
}
