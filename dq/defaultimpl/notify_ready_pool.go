package defaultimpl

import (
	"github.com/easy-go-123/delay-queue/dqdef"
)

func NewNotifyReadyPool(maxCap int) dqdef.ReadyPool {
	if maxCap <= 0 {
		maxCap = 100
	}

	return &notifyReadyPool{
		jobChan: make(chan *dqdef.JobIdentify, maxCap),
	}
}

type notifyReadyPool struct {
	jobChan chan *dqdef.JobIdentify
}

func (impl *notifyReadyPool) NewReadyJob(topic, jobID string) (err error) {
	impl.jobChan <- &dqdef.JobIdentify{
		Topic: topic,
		ID:    jobID,
	}

	return
}

func (impl *notifyReadyPool) JobChan() <-chan *dqdef.JobIdentify {
	return impl.jobChan
}
