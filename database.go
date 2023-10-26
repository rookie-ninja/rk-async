package rkasync

import "gorm.io/gorm/clause"

type Database interface {
	Type() string

	AddJob(job *Job) error

	RegisterProcessor(jobType string, processor Processor)

	GetProcessor(jobType string) Processor

	PickJobToWork() (*Job, error)

	UpdateJobState(job *Job) error

	UpdateJobPayloadAndStep(job *Job) error

	ListJobs(filter *JobFilter) ([]*Job, error)

	GetJob(id string) (*Job, error)

	CancelJobsOverdue(days int, filter *JobFilter) error

	CleanJobs(days int, filter *JobFilter) error
}

type JobFilter struct {
	ClauseList []clause.Expression
	Limit      int
	Order      string
}
