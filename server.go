package async

type Server struct {
	db Database
}

func (s *Server) AddJob(job Job) error {
	return s.db.AddJob(job)
}

func (s *Server) UpdateJobState(job Job, state string) error {
	return s.db.UpdateJobState(job, state)
}

func (s *Server) ListJobs(filter *JobFilter) ([]Job, error) {
	return s.db.ListJobs(filter)
}

func (s *Server) GetJob(id string) (Job, error) {
	return s.db.GetJob(id)
}

func (s *Server) CancelJobsOverdue(days int) error {
	return s.db.CancelJobsOverdue(days)
}

func (s *Server) CleanJobs(days int) error {
	return s.db.CleanJobs(days)
}

func (s *Server) RegisterMarshaller(jobType string, f func(Job) ([]byte, error)) {
	s.db.RegisterMarshaller(jobType, f)
}

func (s *Server) RegisterUnmarshaler(jobType string, f func([]byte) (Job, error)) {
	s.db.RegisterUnmarshaler(jobType, f)
}
