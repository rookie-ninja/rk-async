package main

import (
	"context"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/rookie-ninja/rk-async"
	_ "github.com/rookie-ninja/rk-async/database/mysql"
	"github.com/rookie-ninja/rk-boot/v2"
	"github.com/rookie-ninja/rk-gin/v2/boot"
	_ "github.com/rookie-ninja/rk-gin/v2/boot"
	"github.com/rookie-ninja/rk-gin/v2/middleware/context"
	"go.uber.org/zap"
	"net/http"
)

func main() {
	// Create a new boot instance.
	boot := rkboot.NewBoot()

	// Bootstrap
	boot.Bootstrap(context.Background())

	asyncEntry := rkasync.GetEntry()
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
	res.JobMeta = &rkasync.JobMeta{
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
	*rkasync.JobMeta
}

func (d *DemoJob) Process(ctx context.Context) error {
	d.Message = "Started"
	return nil
}

func (d *DemoJob) Meta() *rkasync.JobMeta {
	return d.JobMeta
}

func (d *DemoJob) Unmarshal(b []byte, meta *rkasync.JobMeta) (rkasync.Job, error) {
	res := &DemoJob{}

	if err := json.Unmarshal(b, res); err != nil {
		return nil, err
	}

	res.JobMeta = meta
	return res, nil
}

func (d *DemoJob) Marshal() ([]byte, error) {
	return json.Marshal(d)
}
