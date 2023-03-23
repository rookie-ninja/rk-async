package rkasync

import (
	"context"
	"gorm.io/datatypes"
	"sync"
	"time"
)

const (
	JobStateCreated  = "created"
	JobStateRunning  = "running"
	JobStateCanceled = "canceled"
	JobStateSuccess  = "success"
	JobStateFailed   = "failed"
)

type Job struct {
	// do not edit
	Id        string    `json:"id" yaml:"id" gorm:"primaryKey"`
	State     string    `json:"state" yaml:"state" gorm:"index"`
	CreatedAt time.Time `yaml:"createdAt" json:"createdAt" attr:"-"`
	UpdatedAt time.Time `yaml:"updatedAt" json:"updatedAt"`

	// edit
	Type   string `json:"type" yaml:"type" gorm:"index"`
	UserId string `json:"userId" yaml:"userId" gorm:"index"`

	Filter string `json:"filter" yaml:"filter" gorm:"text"`

	Steps   datatypes.JSONType[[]*Step]     `json:"steps" yaml:"steps"`
	Payload datatypes.JSONType[interface{}] `json:"payload" yaml:"payload"`
}

func (j *Job) TableName() string {
	return "rk_async_job"
}

type Step struct {
	Index      int       `json:"index" yaml:"index"`
	Name       string    `json:"id" yaml:"id"`
	State      string    `json:"state" yaml:"state"`
	StartedAt  time.Time `yaml:"startedAt" json:"startedAt"`
	ElapsedSec float64   `yaml:"elapsedSec" json:"elapsedSec"`
	Output     []string  `yaml:"output" json:"output"`

	PersistFunc func() `json:"-" yaml:"-"`

	Lock sync.Mutex `json:"-" yaml:"-"`
}

func (s *Step) AddOutput(inputs ...string) {
	s.Lock.Lock()
	defer s.Lock.Unlock()

	s.Output = append(s.Output, inputs...)

	s.PersistFunc()
}

func (s *Step) Finish(state string) {
	s.State = state
	s.ElapsedSec = time.Now().Sub(s.StartedAt).Seconds()
	s.PersistFunc()
}

type UpdateJobFunc func(j *Job) error

type Processor interface {
	Process(context.Context, *Job, UpdateJobFunc) error
}
