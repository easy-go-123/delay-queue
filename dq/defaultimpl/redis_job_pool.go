package defaultimpl

import (
	"context"
	"errors"
	"time"

	"github.com/easy-go-123/delay-queue/dqdef"
	"github.com/go-redis/redis/v8"
	"github.com/sgostarter/libeasygo/cuserror"
	"github.com/sgostarter/libeasygo/helper"
	"github.com/vmihailenco/msgpack"
)

func NewRedisJobPool(redisCli *redis.Client) dqdef.JobPool {
	return &redisJobPoolImpl{
		redisCli: redisCli,
	}
}

type redisJobPoolImpl struct {
	redisCli *redis.Client
	dqName   string
}

func (impl *redisJobPoolImpl) SetDelayQueueName(name string) {
	impl.dqName = name
}

func (impl *redisJobPoolImpl) GetJob(ctx context.Context, jobID string, jobIn *dqdef.Job) (job *dqdef.Job, err error) {
	var bs []byte

	err = helper.RunWithTimeout4Redis(ctx, func(ctx context.Context) error {
		bs, err = impl.redisCli.Get(ctx, impl.jobRedisKey(jobID)).Bytes()

		return err
	})

	if err != nil {
		if errors.Is(err, redis.Nil) {
			err = nil
		}

		return
	}

	if jobIn != nil {
		job = jobIn
	} else {
		job = &dqdef.Job{}
	}

	err = msgpack.Unmarshal(bs, job)

	return
}

func (impl *redisJobPoolImpl) SaveJob(ctx context.Context, job *dqdef.Job, afterHook func() error) (err error) {
	d, err := msgpack.Marshal(job)
	if err != nil {
		return
	}

	var expiration time.Duration
	if afterHook != nil {
		expiration = 5 * time.Second
	}

	err = helper.RunWithTimeout4Redis(ctx, func(ctx context.Context) error {
		return impl.redisCli.Set(ctx, impl.jobRedisKey(job.ID), d, expiration).Err()
	})

	if err != nil {
		return
	}

	if afterHook == nil {
		return
	}

	if err != nil {
		return
	}

	err = afterHook()

	if err != nil {
		return
	}

	err = helper.RunWithTimeout4Redis(ctx, func(ctx context.Context) error {
		return impl.redisCli.Persist(ctx, impl.jobRedisKey(job.ID)).Err()
	})

	return err
}

func (impl *redisJobPoolImpl) RemoveJobByDQ(ctx context.Context, jobID string) (err error) {
	return helper.RunWithTimeout4Redis(ctx, func(ctx context.Context) error {
		var n int64
		n, err = impl.redisCli.Del(ctx, impl.jobRedisKey(jobID)).Result()
		if err != nil {
			return nil
		}
		if n == 0 {
			return cuserror.NewWithErrorMsg("not exists")
		}

		return nil
	})
}

func (impl *redisJobPoolImpl) RemoveJob(ctx context.Context, jobID string) (err error) {
	return impl.RemoveJobByDQ(ctx, jobID)
}

func (impl *redisJobPoolImpl) jobRedisKey(jobID string) string {
	return "dqJob:" + impl.dqName + ":" + jobID
}
