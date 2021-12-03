package dq

import (
	"context"
	"testing"
	"time"

	"github.com/easy-go-123/delay-queue/dqdef"
	"github.com/stretchr/testify/assert"
)

func testMakeBlockDelayQueue(ctx context.Context, t *testing.T) *blockDelayQueueImpl {
	dq := testMakeDelayQueue(ctx, t)

	return NewBlockDelayQueue(dq).(*blockDelayQueueImpl)
}

func TestBDQ5(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bq := testMakeBlockDelayQueue(ctx, t)

	tBody := &TestBody{
		Times: []time.Duration{
			time.Second, 2 * time.Second, 3 * time.Second, 4 * time.Second, 5 * time.Second,
		},
		Index: 0,
	}

	startTime := time.Now()

	err := bq.PushJob(&dqdef.Job{
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

		ok, err := bq.BlockProcessJobOnce(func(job *dqdef.Job) (newJob *dqdef.Job, err error) {
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
		}, time.Hour, job, "topic")
		assert.Nil(t, err)
		assert.True(t, ok)
	}

	bq.GetDelayQueue().Wait()
}
