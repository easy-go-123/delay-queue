<div align="center">
  <img src="https://github.com/easy-go-123/delay-queue/workflows/ut/badge.svg?branch=main&event=push" alt="Unit Test">
  <img src="https://github.com/easy-go-123/delay-queue/workflows/golangci-lint/badge.svg?branch=main&event=push" alt="GolangCI Linter">
</div>

## Golang 延迟队列

---


依赖`Redis`实现的延迟队列。
> 完成队列和任务储存默认使用`redis`实现，可定制

## 应用场景

---

* 订单30分钟未支付，自动关闭
* 订单完成后，如果用户5天未评价，5天后自动好评
* 会员到期前15天，到期前3天分别发送短信提醒
## 使用

```golang
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

func testMakeDelayQueue(ctx context.Context, t *testing.T) dqdef.BlockDelayQueue {
	cfg := ut.SetupUTConfig4Redis(t)
	redisCli, err := helper.NewRedisClient(cfg.RedisDNS)
	assert.Nil(t, err)

	ks, _ := redisCli.Keys(context.Background(), "job*").Result()
	redisCli.Del(context.Background(), ks...)
	ks, _ = redisCli.Keys(context.Background(), "topic*").Result()
	redisCli.Del(context.Background(), ks...)
	redisCli.Del(context.Background(), "bucket1")

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

```

## 实现原理

---

利用`Redis`的有序集合，`member`为`JobID`, `score`为任务执行的时间戳

### 普通任务

任务到期后，到期任务`ID`会放入任务待命池(`ReadyPool`)中, 由调用方获取后执行 -默认`ReadyPool`为`redis list`，调用放使用`bpop`阻塞获取

### 安全任务 - 需要设置`Job`的`TTR`

任务到期后，到期任务`ID`会放入任务待命池(`ReadyPool`)中, 同时该任务会以 `当前时间+TTR` 参数继续放入延迟队列中，如果在再次到期(`当前时间+TTR`)
之前任务执行完毕，则需要主动去延迟队列中摘除当前任务(使用`dqdef.BlockDelayQueue`不需要主动摘除任务)，否则任务再次到期后，会重新进入`ReadyPool`中，供使用者调度。

注意：安全队列保证任务至少调度一次，不保证不会重复调度
