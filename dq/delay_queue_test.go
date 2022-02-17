package dq

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/easy-go-123/delay-queue/dq/defaultimpl"
	"github.com/easy-go-123/delay-queue/dqdef"
	"github.com/sgostarter/i/logger"
	"github.com/sgostarter/libeasygo/helper"
	"github.com/sgostarter/libeasygo/ut"
	"github.com/stretchr/testify/assert"
)

func testMakeDelayQueue(ctx context.Context, t *testing.T) *delayQueueImpl {
	cfg := ut.SetupUTConfig4Redis(t)
	redisCli, err := helper.NewRedisClient(cfg.RedisDNS)
	assert.Nil(t, err)

	ks, _ := redisCli.Keys(ctx, "job*").Result()
	redisCli.Del(ctx, ks...)
	ks, _ = redisCli.Keys(ctx, "topic*").Result()
	redisCli.Del(ctx, ks...)
	redisCli.Del(ctx, "bucket1")

	return NewDelayQueue(ctx, redisCli, "bucket1",
		defaultimpl.NewRedisReadyQueue(ctx, redisCli), defaultimpl.NewRedisJobPool(redisCli, "job_"),
		logger.NewWrapper(logger.NewCommLogger(&logger.FmtRecorder{}))).(*delayQueueImpl)
}

func TestDQ1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dq := testMakeDelayQueue(ctx, t)

	err := dq.JobPush(&dqdef.Job{
		Topic: "topic1",
		ID:    "1",
		Delay: time.Now().Add(time.Second * 2),
		TTR:   0,
		Body:  "111",
	})
	assert.Nil(t, err)

	blockFetcher, _ := NewBlockDelayQueueWithDQ(dq).(*blockDelayQueueImpl)

	job, err := blockFetcher.jobBPopEx(time.Second, nil, "topic1", "topic2")
	assert.Nil(t, err)
	assert.Nil(t, job)

	job, err = blockFetcher.jobBPopEx(2*time.Second, nil, "topic1", "topic2")
	assert.Nil(t, err)
	assert.NotNil(t, job)

	job2, err := blockFetcher.jobBPopEx(3*time.Second, nil, "topic1", "topic2")
	assert.Nil(t, err)
	assert.Nil(t, job2)

	dq.JobDone(job.ID)
}

func TestDQ2(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	dq := testMakeDelayQueue(ctx, t)

	err := dq.JobPush(&dqdef.Job{
		Topic: "topic1",
		ID:    "1",
		Delay: time.Now().Add(time.Second * 2),
		TTR:   time.Second * 2,
		Body:  "111",
	})
	assert.Nil(t, err)

	blockFetcher, _ := NewBlockDelayQueueWithDQ(dq).(*blockDelayQueueImpl)

	job, err := blockFetcher.jobBPopEx(time.Second, nil, "topic1", "topic2")
	assert.Nil(t, err)
	assert.Nil(t, job)

	job, err = blockFetcher.jobBPopEx(2*time.Second, nil, "topic1", "topic2")
	assert.Nil(t, err)
	assert.NotNil(t, job)

	job, err = blockFetcher.jobBPopEx(3*time.Second, nil, "topic1", "topic2")
	assert.Nil(t, err)
	assert.NotNil(t, job)

	dq.JobDone(job.ID)
}

func TestDQ3(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 16*time.Second)
	defer cancel()

	dq := testMakeDelayQueue(ctx, t)

	err := dq.JobPush(&dqdef.Job{
		Topic: "topic1",
		ID:    "1",
		Delay: time.Now().Add(10 * time.Second),
		TTR:   0,
		Body:  "10s",
	})
	assert.Nil(t, err)

	err = dq.JobPush(&dqdef.Job{
		Topic: "topic2",
		ID:    "2",
		Delay: time.Now().Add(12 * time.Second),
		TTR:   0,
		Body:  "12s",
	})
	assert.Nil(t, err)

	err = dq.JobPush(&dqdef.Job{
		Topic: "topic",
		ID:    "3",
		Delay: time.Now().Add(3 * time.Second),
		TTR:   0,
		Body:  "3s",
	})
	assert.Nil(t, err)

	err = dq.JobPush(&dqdef.Job{
		Topic: "topic",
		ID:    "4_1",
		Delay: time.Now().Add(14 * time.Second),
		TTR:   0,
		Body:  "14s",
	})
	assert.Nil(t, err)

	err = dq.JobPush(&dqdef.Job{
		Topic: "topic",
		ID:    "4_2",
		Delay: time.Now().Add(14 * time.Second),
		TTR:   0,
		Body:  "14s",
	})
	assert.Nil(t, err)

	err = dq.JobPush(&dqdef.Job{
		Topic: "topic",
		ID:    "5",
		Delay: time.Now().Add(5 * time.Second),
		TTR:   0,
		Body:  "5s",
	})
	assert.Nil(t, err)

	start := time.Now()

	blockFetcher, _ := NewBlockDelayQueueWithDQ(dq).(*blockDelayQueueImpl)

	go func() {
		for {
			job, _ := blockFetcher.jobBPopEx(time.Minute, nil, "topic1", "topic2", "topic")
			if job != nil {
				t.Logf("%s %v", job.ID, time.Since(start))
				dq.JobDone(job.ID)
			}
		}
	}()

	dq.Wait()
}

func TestDQ4(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fnProcessBody := func(s string) (string, time.Duration) {
		ps := strings.Split(s, ":")
		psT := strings.Split(ps[0], ",")
		idx, err := strconv.Atoi(ps[1])

		if err != nil {
			t.Log(err)

			return "", 0
		}

		if idx >= len(psT) {
			return "", 0
		}

		d, err := time.ParseDuration(psT[idx])
		if err != nil {
			return "", 0
		}

		return ps[0] + ":" + strconv.Itoa(idx+1), d
	}

	dq := testMakeDelayQueue(ctx, t)

	body := "1s,2s,3s,4s,5s,6s:0"

	startTime := time.Now()

	for {
		var d time.Duration
		body, d = fnProcessBody(body)

		if body == "" {
			t.Log("out,out")

			break
		}

		err := dq.JobPush(&dqdef.Job{
			Topic: "topic",
			ID:    "1",
			Delay: time.Now().Add(d),
			TTR:   0,
			Body:  body,
		})
		assert.Nil(t, err)

		blockFetcher, _ := NewBlockDelayQueueWithDQ(dq).(*blockDelayQueueImpl)

		var job *dqdef.Job
		job, err = blockFetcher.jobBPopEx(time.Hour, nil, "topic")
		assert.Nil(t, err)
		assert.NotNil(t, job)

		t.Logf("%v", time.Since(startTime))
		startTime = time.Now()

		body = job.Body
	}

	dq.Wait()
}

type TestBody struct {
	Times []time.Duration
	Index int
}

func TestDQ5(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dq := testMakeDelayQueue(ctx, t)

	tBody := &TestBody{
		Times: []time.Duration{
			time.Second, 2 * time.Second, 3 * time.Second, 4 * time.Second, 5 * time.Second,
		},
		Index: 0,
	}

	startTime := time.Now()

	blockFetcher, _ := NewBlockDelayQueueWithDQ(dq).(*blockDelayQueueImpl)

	for {
		err := dq.JobPush(&dqdef.Job{
			Topic: "topic",
			ID:    "1",
			Delay: time.Now().Add(tBody.Times[tBody.Index]),
			TTR:   0,
			BodyO: tBody,
		})
		assert.Nil(t, err)

		job := &dqdef.Job{
			BodyO: &TestBody{},
		}
		job, err = blockFetcher.jobBPopEx(time.Hour, job, "topic")
		assert.Nil(t, err)
		assert.NotNil(t, job)

		t.Logf("%v", time.Since(startTime))
		startTime = time.Now()

		tBody, _ = job.BodyO.(*TestBody)
		tBody.Index++

		if tBody.Index >= len(tBody.Times) {
			t.Log("out,out")

			break
		}
	}

	dq.Wait()
}
