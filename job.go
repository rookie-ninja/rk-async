package rkasync

import (
	"context"
	"fmt"
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
	Index      int       `json:"index" yaml:"index"`
	Name       string    `json:"id" yaml:"id"`
	State      string    `json:"state" yaml:"state"`
	StartedAt  time.Time `yaml:"startedAt" json:"startedAt"`
	UpdatedAt  time.Time `yaml:"updatedAt" json:"updatedAt"`
	ElapsedSec float64   `yaml:"elapsedSec" json:"elapsedSec"`
	Output     []string  `yaml:"output" json:"output"`
}

func (s *Step) SuccessOutput(output string, startTime time.Time) {
	s.UpdatedAt = time.Now()
	s.Output = append(s.Output, fmt.Sprintf("%s, elapsedSec:%.2f", output, s.UpdatedAt.Sub(startTime).Seconds()))
}

func (s *Step) FailedOutput(output string, startTime time.Time) {
	s.UpdatedAt = time.Now()
	s.State = JobStateFailed
	s.Output = append(s.Output, fmt.Sprintf("%s, elapsedSec:%.2f", output, s.UpdatedAt.Sub(startTime).Seconds()))
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
	case "created":
		if newState == JobStateRunning || newState == JobStateCanceled {
			return true
		}

		return false
	case "running":
		if newState == JobStateCreated {
			return false
		}
		return true
	case "canceled", "success", "failed":
		return false
	}

	return false
}
