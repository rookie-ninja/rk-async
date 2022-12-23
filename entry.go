package async

type Entry struct {
	worker Worker
	server Server
}

func (e *Entry) AddJob() {

}

func (e *Entry) ListJobs() {}

func (e *Entry) GetJob() {}

func (e *Entry) CancelJob() {}

func (e *Entry) CancelJobsOverdue() {}

func (e *Entry) CleanJobs() {}
