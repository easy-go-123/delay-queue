package dq

import (
	"context"
	"time"

	"github.com/easy-go-123/delay-queue/dqdef"
)

func NewBlockDelayQueue(dq dqdef.DelayQueue) dqdef.BlockDelayQueue {
	if dq == nil || dq.GetReadyPool() == nil {
		return nil
	}

	rp := dq.GetReadyPool()
	rpFetcher, ok := rp.(dqdef.ReadyPoolFetcher)

	if !ok || rpFetcher == nil {
		return nil
	}

	return &blockDelayQueueImpl{
		dq:        dq,
		rpFetcher: rpFetcher,
	}
}

type blockDelayQueueImpl struct {
	dq        dqdef.DelayQueue
	rpFetcher dqdef.ReadyPoolFetcher
}

func (impl *blockDelayQueueImpl) GetDelayQueue() dqdef.DelayQueue {
	return impl.dq
}

func (impl *blockDelayQueueImpl) PushJob(job *dqdef.Job) error {
	if job.TTR != 0 {
		return dqdef.ErrSafeJob
	}

	return impl.dq.JobPush(job)
}

func (impl *blockDelayQueueImpl) PushSafeJob(job *dqdef.Job) error {
	if job.TTR <= 0 {
		return dqdef.ErrNoSafeJob
	}

	return impl.dq.JobPush(job)
}

func (impl *blockDelayQueueImpl) BlockProcessJobOnce(f dqdef.FNProcessJob, timeout time.Duration, jobIn *dqdef.Job, topics ...string) (ok bool, err error) {
	job, err := impl.jobBPopEx(timeout, jobIn, topics...)
	if err != nil {
		return
	}

	if job == nil {
		return
	}

	ok = true
	jobID := job.ID
	newJob, err := f(job)

	if err != nil {
		return
	}

	impl.dq.JobDone(jobID)

	if newJob != nil {
		err = impl.dq.JobPush(newJob)
	}

	return
}

func (impl *blockDelayQueueImpl) jobBPopEx(timeout time.Duration, jobIn *dqdef.Job, topics ...string) (job *dqdef.Job, err error) {
	b := time.Now()
	tm := timeout

	for {
		job, err = impl.bPopEx(tm, jobIn, topics...)
		if err != nil {
			return
		}

		if job != nil {
			return
		}

		if timeout <= 0 {
			return
		}

		tm = timeout - time.Since(b)
		if tm <= 0 {
			return
		}
	}
}

func (impl *blockDelayQueueImpl) bPopEx(timeout time.Duration, jobIn *dqdef.Job, topics ...string) (job *dqdef.Job, err error) {
	start := time.Now()

	jid, err := impl.rpFetcher.GetReadyJob(timeout, topics...)
	if err != nil {
		return
	}

	if jid == nil {
		return
	}

	to := timeout - time.Since(start)
	if to <= 0 {
		err = dqdef.ErrTimeout

		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), to)
	defer cancel()

	job, err = impl.dq.GetJobPool().GetJob(ctx, jid.ID, jobIn)
	if err != nil {
		return
	}

	if job == nil {
		impl.dq.JobDone(jid.ID)
	}

	return
}

func (impl *blockDelayQueueImpl) StopAndWait() {
	impl.dq.StopAndWait()
}

func (impl *blockDelayQueueImpl) Wait() {
	impl.dq.Wait()
}
