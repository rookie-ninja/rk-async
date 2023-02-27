package rkasync

import (
	"context"
	"gorm.io/datatypes"
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
	UpdatedAt time.Time `yaml:"updatedAt" json:"updatedAt" attr:"-"`

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
	Index      int           `json:"index" yaml:"index"`
	Name       string        `json:"id" yaml:"id"`
	State      string        `json:"state" yaml:"state"`
	StartedAt  time.Time     `yaml:"startedAt" json:"startedAt"`
	UpdatedAt  time.Time     `yaml:"updatedAt" json:"updatedAt"`
	ElapsedSec float64       `yaml:"elapsedSec" json:"elapsedSec"`
	Output     []*StepOutput `yaml:"output" json:"output"`
}

type StepOutput struct {
	Message    string  `json:"message,omitempty"`
	ElapsedSec float64 `json:"elapsedSec"`
	Success    bool    `json:"success"`
	RetryCount int     `json:"retryCount"`
	Error      string  `json:"error,omitempty"`
}

func (s *Step) SuccessOutput(output *StepOutput, startTime time.Time) {
	s.UpdatedAt = time.Now()
	if output == nil {
		return
	}
	output.Success = true
	output.ElapsedSec = s.UpdatedAt.Sub(startTime).Seconds()
	s.Output = append(s.Output, output)
}

func (s *Step) FailedOutput(output *StepOutput, err error, startTime time.Time) {
	s.UpdatedAt = time.Now()
	s.State = JobStateFailed
	if output == nil {
		return
	}
	if err != nil {
		output.Error = err.Error()
	}
	output.Success = false
	output.ElapsedSec = s.UpdatedAt.Sub(startTime).Seconds()
	s.Output = append(s.Output, output)
}

func (s *Step) Finish() {
	s.UpdatedAt = time.Now()
	s.ElapsedSec = s.UpdatedAt.Sub(s.StartedAt).Seconds()
}

type UpdateJobFunc func(j *Job, state string) error

type Processor interface {
	Process(context.Context, *Job, UpdateJobFunc) error
}

func JobNewStateAllowed(oldState, newState string) bool {
	switch oldState {
	case JobStateCreated:
		if newState == JobStateRunning || newState == JobStateCanceled || newState == JobStateCreated {
			return true
		}

		return false
	case JobStateRunning:
		if newState == JobStateCreated || newState == JobStateRunning {
			return false
		}
		return true
	case JobStateCanceled, JobStateSuccess, JobStateFailed:
		return false
	}

	return false
}
