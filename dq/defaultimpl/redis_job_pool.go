package defaultimpl

import (
	"context"
	"errors"
	"time"

	"github.com/easy-go-123/delay-queue/dqdef"
	"github.com/go-redis/redis/v8"
	"github.com/sgostarter/libeasygo/helper"
	"github.com/vmihailenco/msgpack"
)

func NewRedisJobPool(redisCli *redis.Client, jobPrefix string) dqdef.JobPool {
	return &redisJobPoolImpl{
		redisCli:  redisCli,
		jobPrefix: jobPrefix,
	}
}

type redisJobPoolImpl struct {
	redisCli  *redis.Client
	jobPrefix string
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

func (impl *redisJobPoolImpl) RemoveJob(ctx context.Context, jobID string) (err error) {
	return helper.RunWithTimeout4Redis(ctx, func(ctx context.Context) error {
		return impl.redisCli.Del(ctx, impl.jobRedisKey(jobID)).Err()
	})
}

func (impl *redisJobPoolImpl) jobRedisKey(jobID string) string {
	return impl.jobPrefix + jobID
}
