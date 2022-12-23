package async

import (
	"context"
	"encoding/json"
	"github.com/rookie-ninja/rk-entry/v2/entry"
)

func init() {
	rkentry.RegisterUserEntryRegFunc(RegisterEntriesFromConfig)
}

var (
	dbRegFuncM = map[string]func(map[string]string) Database{}
)

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
		var db Database
		var dbAddr string
		if config.Async.Database.MySql.Enabled {
			f := dbRegFuncM["MySQL"]
			db = f(map[string]string{
				"entryName": config.Async.Database.MySql.EntryName,
				"database":  config.Async.Database.MySql.Database,
			})
		}

		if db == nil {
			return res
		}

		// logger
		logger := rkentry.GlobalAppCtx.GetLoggerEntry(config.Async.Logger)
		if logger == nil {
			logger = rkentry.GlobalAppCtx.GetLoggerEntryDefault()
		}

		// event
		event := rkentry.GlobalAppCtx.GetEventEntry(config.Async.Event)
		if event == nil {
			event = rkentry.GlobalAppCtx.GetEventEntryDefault()
		}

		// worker
		var worker Worker
		if config.Async.Worker.Local.Enabled {
			worker = &LocalWorker{
				db:     db,
				logger: logger,
				event:  event,
			}
		}

		entry := &Entry{
			db:     db,
			dbAddr: dbAddr,
			server: &Server{
				db: db,
			},
			worker: worker,
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
	dbAddr string
	worker Worker
	server *Server
}

func (e *Entry) Bootstrap(ctx context.Context) {
	e.worker.Start()
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
		"dbAddr": e.dbAddr,
	}

	b, _ := json.Marshal(m)
	return string(b)
}

func (e *Entry) Worker() Worker {
	return e.worker
}

func (e *Entry) Server() *Server {
	return e.server
}
