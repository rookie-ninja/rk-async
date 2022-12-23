package async

type Database interface {
	Type() string

	RegisterJob(job Job)

	AddJob(job Job) error

	PickJobToWork() (Job, error)

	UpdateJobState(job Job, state string) error

	ListJobs(filter *JobFilter) ([]Job, error)

	GetJob(id string) (Job, error)

	CancelJobsOverdue(days int) error

	CleanJobs(days int) error
}

type UnmarshalerFunc func([]byte, *JobMeta) (Job, error)

func NewJobFilter() *JobFilter {
	return &JobFilter{
		TypeList:     []string{},
		UserList:     []string{},
		ClassList:    []string{},
		CategoryList: []string{},
	}
}

type JobFilter struct {
	TypeList     []string
	UserList     []string
	ClassList    []string
	CategoryList []string
}

func (f *JobFilter) AddType(in string) {
	f.TypeList = append(f.TypeList, in)
}

func (f *JobFilter) AddUser(in string) {
	f.UserList = append(f.UserList, in)
}

func (f *JobFilter) AddClass(in string) {
	f.ClassList = append(f.ClassList, in)
}

func (f *JobFilter) AddCategory(in string) {
	f.CategoryList = append(f.CategoryList, in)
}
