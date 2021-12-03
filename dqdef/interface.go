package dqdef

import (
	"context"
	"time"
)

type JobPool interface {
	SaveJob(ctx context.Context, job *Job, afterHook func() error) (err error)
	GetJob(ctx context.Context, jobID string, jobIn *Job) (job *Job, err error)
	RemoveJob(ctx context.Context, jobID string) (err error)
}

type ReadyPool interface {
	NewReadyJob(topic, jobID string) (err error)
}

type JobIdentify struct {
	Topic string
	ID    string
}

type ReadyPoolFetcher interface {
	GetReadyJob(timeout time.Duration, topics ...string) (jid *JobIdentify, err error)
}

type ReadyPoolNotifier interface {
	JobChan() <-chan *JobIdentify
}

type DelayQueue interface {
	GetReadyPool() ReadyPool
	GetJobPool() JobPool

	JobPush(job *Job) error
	JobDone(jobID string)

	StopAndWait()
	Wait()
}

// FNProcessJob
// 返回值err非空，则安全Job会被再次调度，非安全Job终止继续操作
// 返回值newJob非空，则新的任务会加入延迟队列
// 注意: 会调用不能添加和当前job相同ID的新任务到延迟队列
type FNProcessJob func(job *Job) (newJob *Job, err error)

type BlockDelayQueue interface {
	PushJob(job *Job) error

	// PushSafeJob job到期后会归到完成池，但是仍然会在延迟队列保持TTR的时间，如果TTR时间过后
	// 还没有被调用者主动从延迟队列删除，则会重新调度到完成池.
	PushSafeJob(job *Job) error

	BlockProcessJobOnce(f FNProcessJob, timeout time.Duration, jobIn *Job, topics ...string) (ok bool, err error)

	StopAndWait()
	Wait()
}
