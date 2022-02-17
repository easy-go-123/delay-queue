package easydq

import (
	"context"
	"time"

	"github.com/easy-go-123/delay-queue/dq"
	"github.com/easy-go-123/delay-queue/dq/defaultimpl"
	"github.com/easy-go-123/delay-queue/dqdef"
	"github.com/go-redis/redis/v8"
	"github.com/sgostarter/i/logger"
)

func NewBlockRedisDQ(ctx context.Context, redisCli *redis.Client, bucketName string,
	log logger.Wrapper) dqdef.BlockDelayQueue {
	return &blockRedisDQImpl{
		bdq: dq.NewBlockDelayQueueWithDQ(dq.NewDelayQueue(ctx, redisCli, bucketName, defaultimpl.NewRedisReadyQueue(ctx, redisCli),
			defaultimpl.NewRedisJobPool(redisCli, "job_"), log)),
	}
}

type blockRedisDQImpl struct {
	bdq dqdef.BlockDelayQueue
}

func (impl *blockRedisDQImpl) GetDelayQueue() dqdef.DelayQueue {
	return impl.bdq.GetDelayQueue()
}

func (impl *blockRedisDQImpl) PushJob(job *dqdef.Job) error {
	return impl.bdq.PushJob(job)
}

func (impl *blockRedisDQImpl) PushSafeJob(job *dqdef.Job) error {
	return impl.bdq.PushSafeJob(job)
}

func (impl *blockRedisDQImpl) BlockProcessJobOnce(f dqdef.FNProcessJob, timeout time.Duration, jobIn *dqdef.Job, topics ...string) (ok bool, err error) {
	return impl.bdq.BlockProcessJobOnce(f, timeout, jobIn, topics...)
}

func (impl *blockRedisDQImpl) StopAndWait() {
	impl.bdq.StopAndWait()
}

func (impl *blockRedisDQImpl) Wait() {
	impl.bdq.Wait()
}
