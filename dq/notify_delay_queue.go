package dq

import (
	"context"
	"sync"

	"github.com/easy-go-123/delay-queue/dq/defaultimpl"
	"github.com/easy-go-123/delay-queue/dqdef"
	"github.com/go-redis/redis/v8"
	"github.com/sgostarter/i/l"
)

func NewNotifyDelayQueue(ctx context.Context, redisCli *redis.Client, bucketName, jobPrefix string,
	maxCap int, log l.Wrapper) dqdef.NotifyDelayQueue {
	if log == nil {
		log = l.NewNopLoggerWrapper()
	}

	if ctx == nil || redisCli == nil || bucketName == "" || jobPrefix == "" {
		return nil
	}

	dq := NewDelayQueue(ctx, redisCli, bucketName, defaultimpl.NewNotifyReadyPool(maxCap),
		defaultimpl.NewRedisJobPool(redisCli, jobPrefix), log)

	return NewNotifyDelayQueueWithDQ(ctx, dq, maxCap, log)
}

func NewNotifyDelayQueueWithDQ(ctx context.Context, dq dqdef.DelayQueue, maxCap int, log l.Wrapper) dqdef.NotifyDelayQueue {
	if dq == nil || dq.GetReadyPool() == nil {
		return nil
	}

	rp := dq.GetReadyPool()
	rpFetcher, ok := rp.(dqdef.ReadyPoolNotifier)

	if !ok || rpFetcher == nil {
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)

	if log == nil {
		log = l.NewNopLoggerWrapper()
	}

	impl := &notifyDelayQueueImpl{
		ctx:       ctx,
		ctxCancel: cancel,
		log:       log,
		dq:        dq,
		rpFetcher: rpFetcher,
		jobChan:   make(chan *dqdef.Job, maxCap),
	}

	impl.wg.Add(1)

	go impl.routine()

	return impl
}

type notifyDelayQueueImpl struct {
	wg        sync.WaitGroup
	ctx       context.Context
	ctxCancel context.CancelFunc
	log       l.Wrapper

	dq        dqdef.DelayQueue
	rpFetcher dqdef.ReadyPoolNotifier
	jobChan   chan *dqdef.Job
}

func (impl *notifyDelayQueueImpl) routine() {
	log := impl.log.WithFields(l.FieldString("clsModule", "routine"))

	defer func() {
		impl.wg.Done()
		log.Info("leave")
	}()

	log.Info("enter")

	loop := true
	for loop {
		select {
		case <-impl.ctx.Done():
			loop = false

			break
		case ji := <-impl.rpFetcher.JobChan():
			job, err := impl.dq.GetJobPool().GetJob(impl.ctx, ji.ID, nil)
			if err != nil {
				log.WithFields(l.FieldError("err", err)).Error("getJob")

				continue
			}

			err = impl.dq.GetJobPool().RemoveJobByDQ(impl.ctx, ji.ID)

			if err != nil {
				log.WithFields(l.FieldError("err", err)).Error("removeJob")
			}

			if job != nil {
				impl.jobChan <- job
			}
		}
	}
}

func (impl *notifyDelayQueueImpl) PushJob(job *dqdef.Job) error {
	return impl.dq.JobPush(job)
}

func (impl *notifyDelayQueueImpl) ReadyJobChannel() <-chan *dqdef.Job {
	return impl.jobChan
}

func (impl *notifyDelayQueueImpl) StopAndWait() {
	impl.ctxCancel()
	impl.dq.StopAndWait()
	impl.wg.Wait()
}

func (impl *notifyDelayQueueImpl) Wait() {
	impl.dq.Wait()
	impl.wg.Wait()
}
