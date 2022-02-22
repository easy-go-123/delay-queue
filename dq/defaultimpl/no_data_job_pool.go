package defaultimpl

import (
	"context"

	"github.com/easy-go-123/delay-queue/dqdef"
	"github.com/go-redis/redis/v8"
	"github.com/sgostarter/libeasygo/commerr"
)

const NoDataJobTopic = "__no_data_job__topic__"

func NewNoDataJobPool(redisCli *redis.Client) dqdef.JobPool {
	return &noDataJobPoolImpl{
		redisCli: redisCli,
	}
}

type noDataJobPoolImpl struct {
	redisCli *redis.Client
	dqName   string
}

func (impl *noDataJobPoolImpl) SetDelayQueueName(name string) {
	impl.dqName = name
}

func (impl *noDataJobPoolImpl) SaveJob(ctx context.Context, job *dqdef.Job, afterHook func() error) (err error) {
	if job.TTR > 0 || job.Topic != NoDataJobTopic {
		return commerr.ErrInvalidArgument
	}

	if afterHook != nil {
		return afterHook()
	}

	return nil
}

func (impl *noDataJobPoolImpl) GetJob(ctx context.Context, jobID string, jobIn *dqdef.Job) (job *dqdef.Job, err error) {
	f, err := impl.redisCli.SIsMember(ctx, impl.userRemovedJobSetRedisKey(), jobID).Result()
	if err != nil {
		return
	}

	if f {
		return
	}

	job = &dqdef.Job{
		Topic: NoDataJobTopic,
		ID:    jobID,
	}

	return
}

func (impl *noDataJobPoolImpl) RemoveJobByDQ(ctx context.Context, jobID string) (err error) {
	impl.redisCli.SRem(ctx, impl.userRemovedJobSetRedisKey(), jobID)

	return
}

func (impl *noDataJobPoolImpl) RemoveJob(ctx context.Context, jobID string) (err error) {
	impl.redisCli.SAdd(ctx, impl.userRemovedJobSetRedisKey(), jobID)

	return
}

func (impl *noDataJobPoolImpl) userRemovedJobSetRedisKey() string {
	return "userDeletedJob:" + impl.dqName
}
