package defaultimpl

import (
	"context"
	"testing"
	"time"

	"github.com/sgostarter/libeasygo/helper"
	"github.com/stretchr/testify/assert"
)

func Test1(t *testing.T) {
	t.SkipNow()

	redisCli, err := helper.NewRedisClient("redis://:redis_default_pass@127.0.0.1:8900/8")
	assert.Nil(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)

	defer cancel()

	vs, err := redisCli.BRPop(ctx, 10*time.Second, "l1", "l2").Result()
	assert.Nil(t, err)
	t.Log(vs)
}
