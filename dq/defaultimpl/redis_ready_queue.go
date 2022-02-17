package defaultimpl

import (
	"context"
	"errors"
	"time"

	"github.com/easy-go-123/delay-queue/dqdef"
	"github.com/go-redis/redis/v8"
	"github.com/sgostarter/libeasygo/helper"
)

func NewRedisReadyQueue(ctx context.Context, redisCli *redis.Client) dqdef.ReadyPool {
	return &redisReadyQueueImpl{
		ctx:      ctx,
		redisCli: redisCli,
	}
}

type redisReadyQueueImpl struct {
	ctx      context.Context
	redisCli *redis.Client
}

func (impl *redisReadyQueueImpl) NewReadyJob(topic, jobID string) (err error) {
	return helper.RunWithTimeout4Redis(impl.ctx, func(ctx context.Context) error {
		return impl.redisCli.RPush(ctx, "dqTopic:"+topic, jobID).Err()
	})
}

func (impl *redisReadyQueueImpl) GetReadyJob(timeout time.Duration, topics ...string) (jid *dqdef.JobIdentify, err error) {
	redisTopics := make([]string, len(topics))

	for idx, topic := range topics {
		redisTopics[idx] = "dqTopic:" + topic
	}

	vs, err := impl.redisCli.BLPop(impl.ctx, timeout, redisTopics...).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			err = nil
		}

		return
	}

	if len(vs) == 0 {
		return
	}

	jid = &dqdef.JobIdentify{
		Topic: vs[0],
		ID:    vs[1],
	}

	return
}
