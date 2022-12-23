package async

type Worker interface {
	Start() error

	Database() Database
}

type LocalWorker struct {
	db Database
}

func (w *LocalWorker) Start() error {
	//TODO implement me
	panic("implement me")
}

func (w *LocalWorker) Database() Database {
	return w.db
}
