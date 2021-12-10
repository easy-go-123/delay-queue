package dq

import (
	"context"
	"testing"
	"time"

	"github.com/easy-go-123/delay-queue/dqdef"
	"github.com/sgostarter/i/logger"
	"github.com/sgostarter/libeasygo/helper"
	"github.com/sgostarter/libeasygo/ut"
	"github.com/stretchr/testify/assert"
)

func testMakeNotifyDelayQueue(ctx context.Context, t *testing.T) dqdef.NotifyDelayQueue {
	cfg := ut.SetupUTConfig4Redis(t)
	redisCli, err := helper.NewRedisClient(cfg.RedisDNS)
	assert.Nil(t, err)

	ks, _ := redisCli.Keys(context.Background(), "job*").Result()
	redisCli.Del(context.Background(), ks...)
	ks, _ = redisCli.Keys(context.Background(), "topic*").Result()
	redisCli.Del(context.Background(), ks...)
	redisCli.Del(context.Background(), "bucket1")

	return NewNotifyDelayQueue(ctx, redisCli, "bucket1", "job_", 100,
		logger.NewConsoleLoggerWrapper())
}

func TestNQ3(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 16*time.Second)
	defer cancel()

	dq := testMakeNotifyDelayQueue(ctx, t)

	err := dq.PushJob(&dqdef.Job{
		Topic: "topic1",
		ID:    "1",
		Delay: time.Now().Add(10 * time.Second),
		Body:  "10s",
	})
	assert.Nil(t, err)

	err = dq.PushJob(&dqdef.Job{
		Topic: "topic2",
		ID:    "2",
		Delay: time.Now().Add(12 * time.Second),
		TTR:   0,
		Body:  "12s",
	})
	assert.Nil(t, err)

	err = dq.PushJob(&dqdef.Job{
		Topic: "topic",
		ID:    "3",
		Delay: time.Now().Add(3 * time.Second),
		TTR:   0,
		Body:  "3s",
	})
	assert.Nil(t, err)

	err = dq.PushJob(&dqdef.Job{
		Topic: "topic",
		ID:    "4_1",
		Delay: time.Now().Add(14 * time.Second),
		TTR:   0,
		Body:  "14s",
	})
	assert.Nil(t, err)

	err = dq.PushJob(&dqdef.Job{
		Topic: "topic",
		ID:    "4_2",
		Delay: time.Now().Add(14 * time.Second),
		TTR:   0,
		Body:  "14s",
	})
	assert.Nil(t, err)

	err = dq.PushJob(&dqdef.Job{
		Topic: "topic",
		ID:    "5",
		Delay: time.Now().Add(5 * time.Second),
		TTR:   0,
		Body:  "5s",
	})
	assert.Nil(t, err)

	start := time.Now()

	go func() {
		for {
			job := <-dq.ReadyJobChannel()
			t.Logf("%s %v", job.ID, time.Since(start))
		}
	}()

	dq.Wait()
}
