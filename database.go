package async

import "context"

type Database interface {
	AddJob(job Job) error

	ListJobs() ([]Job, error)

	GetJob(id string) (Job, error)

	CancelJob(id string) error

	CancelJobsOverdue() error

	CleanJobs() error
}

const (
	JobStateCreated  = "created"
	JobStateRunning  = "running"
	JobStateCanceled = "canceled"
	JobStateSuccess  = "success"
	JobStateFailed   = "failed"
)

type Job interface {
	Start(context.Context)

	Cancel(context.Context)

	GetId() string

	GetState() string
}
