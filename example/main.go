package main

import (
	"context"
	"encoding/json"
	"github.com/gin-gonic/gin"
	async "github.com/rookie-ninja/rk-async"
	_ "github.com/rookie-ninja/rk-async/database/mysql"
	"github.com/rookie-ninja/rk-boot/v2"
	_ "github.com/rookie-ninja/rk-gin/v2/boot"
	rkgin "github.com/rookie-ninja/rk-gin/v2/boot"
	rkginctx "github.com/rookie-ninja/rk-gin/v2/middleware/context"
	"go.uber.org/zap"
	"net/http"
)

func main() {
	// Create a new boot instance.
	boot := rkboot.NewBoot()

	// Bootstrap
	boot.Bootstrap(context.Background())

	asyncEntry := async.GetEntry()
	asyncEntry.RegisterJob(NewDemoJob())
	asyncEntry.Worker().Start()

	ginEntry := rkgin.GetGinEntry("example")
	ginEntry.Router.GET("/list-job", func(c *gin.Context) {
		jobList, err := asyncEntry.ListJobs(nil)
		if err != nil {
			rkginctx.GetLogger(c).Error("", zap.Error(err))
			c.JSON(http.StatusInternalServerError, err)
			return
		}

		c.JSON(http.StatusOK, jobList)
	})
	ginEntry.Router.GET("/get-job", func(c *gin.Context) {
		job, err := asyncEntry.GetJob(c.Query("id"))
		if err != nil {
			c.JSON(http.StatusInternalServerError, err)
			return
		}

		c.JSON(http.StatusOK, job)
	})
	ginEntry.Router.GET("/add-job", func(c *gin.Context) {
		job := NewDemoJob()
		err := asyncEntry.AddJob(job)
		if err != nil {
			c.JSON(http.StatusInternalServerError, err)
			return
		}

		c.JSON(http.StatusOK, job)
	})
	ginEntry.Router.GET("/clean", func(c *gin.Context) {
		err := asyncEntry.CleanJobs(0)
		if err != nil {
			c.JSON(http.StatusInternalServerError, err)
			return
		}

		c.JSON(http.StatusOK, "")
	})
	ginEntry.Router.GET("/due", func(c *gin.Context) {
		err := asyncEntry.CancelJobsOverdue(0)
		if err != nil {
			c.JSON(http.StatusInternalServerError, err)
			return
		}

		c.JSON(http.StatusOK, "")
	})

	// Wait for shutdown sig
	boot.WaitForShutdownSig(context.Background())
}

func NewDemoJob() *DemoJob {
	res := &DemoJob{}
	res.JobMeta = &async.JobMeta{
		Type:     "DemoJob",
		User:     "user",
		Class:    "class",
		Category: "category",
	}

	return res
}

type DemoJob struct {
	Message string `json:"message"`
	Error   string `json:"error"`
	*async.JobMeta
}

func (d *DemoJob) Type() string {
	return d.JobMeta.Type
}

func (d *DemoJob) Start(ctx context.Context) error {
	d.Message = "Started"
	return nil
}

func (d *DemoJob) Cancel(ctx context.Context) error {
	d.Message = "Canceled"
	return nil
}

func (d *DemoJob) RecordError(err error) {
	d.Error = err.Error()
}

func (d *DemoJob) GetMeta() *async.JobMeta {
	return d.JobMeta
}

func (d *DemoJob) Unmarshal(b []byte, meta *async.JobMeta) (async.Job, error) {
	if err := json.Unmarshal(b, d); err != nil {
		return nil, err
	}

	d.JobMeta = meta
	return d, nil
}

func (d *DemoJob) Marshal() ([]byte, error) {
	return json.Marshal(d)
}
