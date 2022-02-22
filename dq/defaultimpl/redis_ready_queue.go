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
	dqName   string
}

func (impl *redisReadyQueueImpl) SetDelayQueueName(name string) {
	impl.dqName = name
}

func (impl *redisReadyQueueImpl) NewReadyJob(topic, jobID string) (err error) {
	return helper.RunWithTimeout4Redis(impl.ctx, func(ctx context.Context) error {
		return impl.redisCli.RPush(ctx, impl.toRealTopic(topic), jobID).Err()
	})
}

func (impl *redisReadyQueueImpl) toRealTopic(topic string) string {
	return "dqTopic:" + impl.dqName + ":" + topic
}

func (impl *redisReadyQueueImpl) GetReadyJob(timeout time.Duration, topics ...string) (jid *dqdef.JobIdentify, err error) {
	redisTopics := make([]string, len(topics))

	for idx, topic := range topics {
		redisTopics[idx] = impl.toRealTopic(topic)
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
