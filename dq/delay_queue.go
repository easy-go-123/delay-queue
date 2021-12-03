package dq

import (
	"context"
	"sync"
	"time"

	"github.com/easy-go-123/delay-queue/dqdef"
	"github.com/go-redis/redis/v8"
	"github.com/sgostarter/i/logger"
	"github.com/sgostarter/libeasygo/helper"
)

func NewDelayQueue(ctx context.Context, redisCli *redis.Client, bucketName string,
	readyPool dqdef.ReadyPool, jobPool dqdef.JobPool, log logger.Wrapper) dqdef.DelayQueue {
	ctx, cancel := context.WithCancel(ctx)

	dq := &delayQueueImpl{
		ctx:        ctx,
		ctxCancel:  cancel,
		redisCli:   redisCli,
		bucketName: bucketName,
		readyPool:  readyPool,
		jobPool:    jobPool,
		log:        log,
		newJobChan: make(chan interface{}, 10),
	}

	dq.startCheckRoutine()

	return dq
}

type delayQueueImpl struct {
	wg         sync.WaitGroup
	ctx        context.Context
	ctxCancel  context.CancelFunc
	redisCli   *redis.Client
	bucketName string
	readyPool  dqdef.ReadyPool
	jobPool    dqdef.JobPool
	log        logger.Wrapper
	newJobChan chan interface{}
}

type bucketItem struct {
	timestamp int64
	jobID     string
}

func (dq *delayQueueImpl) GetReadyPool() dqdef.ReadyPool {
	return dq.readyPool
}

func (dq *delayQueueImpl) GetJobPool() dqdef.JobPool {
	return dq.jobPool
}

func (dq *delayQueueImpl) StopAndWait() {
	dq.ctxCancel()
	dq.wg.Wait()
}

func (dq *delayQueueImpl) Wait() {
	dq.wg.Wait()
}

func (dq *delayQueueImpl) JobPush(job *dqdef.Job) error {
	if job.ID == "" || job.Topic == "" || time.Since(job.Delay) >= 0 {
		return dqdef.ErrInvalidJob
	}

	err := dq.jobPool.SaveJob(dq.ctx, job, func() error {
		return dq.save2DelayQueue(job)
	})

	if err == nil {
		dq.newJobChan <- true
	}

	return err
}

func (dq *delayQueueImpl) JobDone(jobID string) {
	_ = dq.jobPool.RemoveJob(dq.ctx, jobID)
	_ = dq.removeFromDelayQueue(jobID)
	dq.newJobChan <- true
}

func (dq *delayQueueImpl) startCheckRoutine() {
	dq.wg.Add(1)

	go func() {
		defer dq.wg.Done()

		log := dq.log.WithFields(logger.FieldString("clsModule", "checkRoutine"))
		log.Info("enterCheckRoutine")

		defer log.Info("leaveCheckRoutine")

		loop := true

		for loop {
			select {
			case <-dq.ctx.Done():
				loop = false

				continue
			case <-dq.newJobChan:

			case <-time.After(time.Second):
				dq.processDelayQueueData(log)
			}
		}
	}()
}

func (dq *delayQueueImpl) processDelayQueueData(log logger.Wrapper) time.Duration {
	for {
		var item *bucketItem
		item, err := dq.getFromDelayQueue()

		if err != nil {
			return 0
		}

		if item == nil {
			return 0
		}

		var job *dqdef.Job
		job, err = dq.jobPool.GetJob(dq.ctx, item.jobID, nil)

		if err != nil {
			return 0
		}

		if job == nil {
			_ = dq.removeFromDelayQueue(item.jobID)

			continue
		}

		if job.Delay.After(time.Now()) {
			return time.Until(job.Delay)
		}

		err = dq.readyPool.NewReadyJob(job.Topic, job.ID)
		if err != nil {
			log.WithFields(logger.FieldError("err", err)).Error("saveJob2ReadyPool")

			return 0
		}

		if job.TTR > 0 {
			err = helper.RunWithTimeout4Redis(dq.ctx, func(ctx context.Context) error {
				return dq.redisCli.ZAdd(ctx, dq.bucketName, &redis.Z{
					Score:  float64(time.Now().Add(job.TTR).Unix()),
					Member: item.jobID,
				}).Err()
			})

			if err != nil {
				log.WithFields(logger.FieldError("err", err)).Error("updateJob")
			}
		} else {
			err = dq.removeFromDelayQueue(item.jobID)
			if err != nil {
				log.WithFields(logger.FieldError("err", err)).Error("updateJob")
			}
		}
	}
}

func (dq *delayQueueImpl) save2DelayQueue(job *dqdef.Job) (err error) {
	err = helper.RunWithTimeout4Redis(dq.ctx, func(ctx context.Context) error {
		return dq.redisCli.ZAdd(ctx, dq.bucketName, &redis.Z{
			Score:  float64(job.Delay.Unix()),
			Member: job.ID,
		}).Err()
	})

	if err != nil {
		dq.log.WithFields(logger.FieldError("err", err)).Error("saveJob2DelayQueue")
	}

	return
}

func (dq *delayQueueImpl) removeFromDelayQueue(jobID string) (err error) {
	err = helper.RunWithTimeout4Redis(dq.ctx, func(ctx context.Context) error {
		return dq.redisCli.ZRem(ctx, dq.bucketName, jobID).Err()
	})

	if err != nil {
		dq.log.WithFields(logger.FieldError("err", err)).Error("removeJobFromDelayQueue")
	}

	return
}

func (dq *delayQueueImpl) getFromDelayQueue() (bi *bucketItem, err error) {
	var zs []redis.Z

	err = helper.RunWithTimeout4Redis(dq.ctx, func(ctx context.Context) error {
		zs, err = dq.redisCli.ZRangeWithScores(ctx, dq.bucketName, 0, 0).Result()

		return err
	})

	if err != nil {
		dq.log.WithFields(logger.FieldError("err", err)).Error("getJobFromDelayQueue")

		return
	}

	if len(zs) == 0 {
		return
	}

	bi = &bucketItem{
		timestamp: int64(zs[0].Score),
		jobID:     zs[0].Member.(string),
	}

	return
}
