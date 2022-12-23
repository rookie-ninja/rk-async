package async

import (
	"context"
	"time"
)

const (
	JobStateCreated  = "created"
	JobStateRunning  = "running"
	JobStateCanceled = "canceled"
	JobStateSuccess  = "success"
	JobStateFailed   = "failed"
)

type JobMeta struct {
	// do not edit
	Id        string    `json:"id" yaml:"id" gorm:"primaryKey"`
	State     string    `json:"state" yaml:"state" gorm:"index"`
	CreatedAt time.Time `yaml:"createdAt" json:"createdAt" attr:"-"`
	UpdatedAt time.Time `yaml:"updatedAt" json:"updatedAt" attr:"-"`

	// edit
	Type     string `json:"type" yaml:"type" gorm:"index"`
	User     string `json:"user" yaml:"user" gorm:"index"`
	Class    string `json:"class" yaml:"class" gorm:"index"`
	Category string `json:"category" yaml:"category" gorm:"index"`
}

type Job interface {
	Start(context.Context) error

	Cancel(context.Context) error

	RecordError(err error)

	GetMeta() *JobMeta

	MarshalFunc() func(Job) ([]byte, error)

	UnmarshalFunc() func([]byte) (Job, error)
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
