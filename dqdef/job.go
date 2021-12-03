package dqdef

import "time"

type Job struct {
	Topic string        `json:"topic" msgpack:"1"`
	ID    string        `json:"id" msgpack:"2"`    // job唯一标识ID
	Delay time.Time     `json:"delay" msgpack:"3"` // 延迟时间
	TTR   time.Duration `json:"ttr" msgpack:"4"`   // 最大任务执行时间, <0代表不设置
	Body  string        `json:"body" msgpack:"5"`
	BodyO interface{}   `json:"body_o" msgpack:"6"`
}
