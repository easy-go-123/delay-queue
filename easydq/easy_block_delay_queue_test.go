package easydq

import (
	"context"
	"testing"
	"time"

	"github.com/easy-go-123/delay-queue/dq/defaultimpl"
	"github.com/easy-go-123/delay-queue/dqdef"
	"github.com/sgostarter/i/l"
	"github.com/sgostarter/libeasygo/helper"
	"github.com/sgostarter/libeasygo/ut"
	"github.com/stretchr/testify/assert"
)

func testMakeDelayQueue(ctx context.Context, t *testing.T) dqdef.BlockDelayQueue {
	cfg := ut.SetupUTConfig4Redis(t)
	redisCli, err := helper.NewRedisClient(cfg.RedisDNS)
	assert.Nil(t, err)

	ks, _ := redisCli.Keys(ctx, "job*").Result()
	redisCli.Del(ctx, ks...)
	ks, _ = redisCli.Keys(ctx, "topic*").Result()
	redisCli.Del(ctx, ks...)
	redisCli.Del(ctx, "bucket1")

	return NewBlockRedisDQ(ctx, redisCli, "bucket1",
		l.NewWrapper(l.NewCommLogger(&l.FmtRecorder{})))
}

func Test1(t *testing.T) {
	bdq := testMakeDelayQueue(context.Background(), t)
	err := bdq.PushJob(&dqdef.Job{
		Topic: "topic1",
		ID:    "id1",
		Delay: time.Now().Add(2 * time.Second),
		TTR:   0,
		Body:  "{}",
	})
	assert.Nil(t, err)
	ok, err := bdq.BlockProcessJobOnce(func(job *dqdef.Job) (newJob *dqdef.Job, err error) {
		return
	}, time.Second, nil, "topic1")
	assert.Nil(t, err)
	assert.False(t, ok)

	ok, err = bdq.BlockProcessJobOnce(func(job *dqdef.Job) (newJob *dqdef.Job, err error) {
		assert.NotNil(t, job)
		assert.Equal(t, job.ID, "id1")

		return
	}, time.Second, nil, "topic1")
	assert.Nil(t, err)
	assert.True(t, ok)

	bdq.StopAndWait()
}

type TestBody struct {
	Times []time.Duration
	Index int
}

func Test2(t *testing.T) {
	bdq := testMakeDelayQueue(context.Background(), t)

	tBody := &TestBody{
		Times: []time.Duration{
			time.Second, 2 * time.Second, 3 * time.Second, 4 * time.Second, 5 * time.Second,
		},
		Index: 0,
	}

	startTime := time.Now()

	err := bdq.PushJob(&dqdef.Job{
		Topic: "topic",
		ID:    "1",
		Delay: time.Now().Add(tBody.Times[tBody.Index]),
		TTR:   0,
		BodyO: tBody,
	})
	assert.Nil(t, err)

	loop := true
	for loop {
		job := &dqdef.Job{
			BodyO: &TestBody{},
		}

		var ok bool
		ok, err = bdq.BlockProcessJobOnce(func(job *dqdef.Job) (newJob *dqdef.Job, err error) {
			t.Logf("%v", time.Since(startTime))
			startTime = time.Now()

			tBody, _ = job.BodyO.(*TestBody)
			tBody.Index++
			if tBody.Index >= len(tBody.Times) {
				t.Log("out,out")
				loop = false

				return
			}

			newJob = job
			newJob.Delay = time.Now().Add(tBody.Times[tBody.Index])

			return
		}, time.Second*10, job, "topic")
		assert.Nil(t, err)

		if err != nil {
			break
		}

		assert.True(t, ok)
	}

	bdq.StopAndWait()
}

func Test3(t *testing.T) {
	ctx := context.Background()

	cfg := ut.SetupUTConfig4Redis(t)
	redisCli, err := helper.NewRedisClient(cfg.RedisDNS)
	assert.Nil(t, err)

	ks, _ := redisCli.Keys(ctx, "job*").Result()
	redisCli.Del(ctx, ks...)
	ks, _ = redisCli.Keys(ctx, "topic*").Result()
	redisCli.Del(ctx, ks...)
	redisCli.Del(ctx, "bucket1")

	dq := NewNoDataBlockRedisDQ(ctx, redisCli, "bucket1",
		l.NewWrapper(l.NewCommLogger(&l.FmtRecorder{})))

	err = dq.PushJob(&dqdef.Job{
		Topic: defaultimpl.NoDataJobTopic,
		ID:    "1:1",
		Delay: time.Now().Add(time.Second),
	})
	assert.Nil(t, err)

	err = dq.PushJob(&dqdef.Job{
		Topic: "1",
		ID:    "1:2",
		Delay: time.Now().Add(time.Second),
	})
	assert.NotNil(t, err)

	err = dq.PushJob(&dqdef.Job{
		Topic: "",
		ID:    "1:2",
		Delay: time.Now().Add(time.Second),
		TTR:   time.Second,
	})
	assert.NotNil(t, err)

	ok, err := dq.BlockProcessJobOnce(func(job *dqdef.Job) (newJob *dqdef.Job, err error) {
		return
	}, time.Second*10, nil, defaultimpl.NoDataJobTopic)
	assert.Nil(t, err)
	assert.True(t, ok)
}
