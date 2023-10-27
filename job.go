package rkasync

import (
	"context"
	"fmt"
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
	Id              string    `json:"id" yaml:"id" gorm:"primaryKey"`
	InvokedRole     string    `json:"invokedRole" yaml:"invokedRole" gorm:"index"`
	InvokedInstance string    `json:"invokedInstance" yaml:"invokedInstance" gorm:"index"`
	State           string    `json:"state" yaml:"state" gorm:"index"`
	CreatedAt       time.Time `yaml:"createdAt" json:"createdAt" attr:"-"`
	UpdatedAt       time.Time `yaml:"updatedAt" json:"updatedAt"`

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

type NewRecorderF func() *Recorder

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

func (s *Step) NewRecorder() *Recorder {
	s.Lock.Lock()
	defer s.Lock.Unlock()

	s.Output = append(s.Output, "")

	return &Recorder{
		index:     len(s.Output) - 1,
		startTime: time.Now(),
		step:      s,
	}
}

func (s *Step) Finish(state string) {
	s.Lock.Lock()
	defer s.Lock.Unlock()

	s.State = state
	s.ElapsedSec = time.Now().Sub(s.StartedAt).Seconds()
	s.PersistFunc()
}

type UpdateJobFunc func(j *Job) error

type Processor interface {
	Process(context.Context, *Job, UpdateJobFunc) error
}

type Recorder struct {
	index     int
	startTime time.Time
	step      *Step
}

func (r *Recorder) Title(s string) {
	output := fmt.Sprintf("👉🏻 %s", s)

	r.step.Lock.Lock()
	defer r.step.Lock.Unlock()

	if r.step != nil && len(r.step.Output) > r.index {
		r.step.Output[r.index] = output
	}

	r.step.PersistFunc()
}

func (r *Recorder) Warn(s string) {
	output := fmt.Sprintf("⚠️️ [%s] %s", time.Duration(time.Now().Sub(r.startTime).Seconds())*time.Second, s)

	r.step.Lock.Lock()
	defer r.step.Lock.Unlock()

	if r.step != nil && len(r.step.Output) > r.index {
		r.step.Output[r.index] = output
	}

	r.step.PersistFunc()
}

func (r *Recorder) Info(s string) {
	output := fmt.Sprintf("[%s] %s", time.Duration(time.Now().Sub(r.startTime).Seconds())*time.Second, s)

	r.step.Lock.Lock()
	defer r.step.Lock.Unlock()

	if r.step != nil && len(r.step.Output) > r.index {
		r.step.Output[r.index] = output
	}

	r.step.PersistFunc()
}
