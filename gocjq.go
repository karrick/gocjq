package gocjq

import (
	"fmt"
)

////////////////////////////////////////

// JobQueue allows enqueuing and dequeueing of jobs. The caller MUST
// invoke Quit method when the queue is no longer needed.
type JobQueue interface {
	Enqueue(interface{})
	Dequeue() interface{}
	Input() chan<- interface{}
	Output() <-chan interface{}
	Quit()
}

type jobQueue struct {
	input, output       chan interface{}
	stages              []*stage
	sump                bool
	terminate, finished chan struct{}
}

// NewQueue creates a job queue. New jobs are enqueued with its
// Enqueue method, are processed by the specified job stages, then
// sent to the client output channel if specified. Once the queue is no
// longer needed, the client MUST call the Quit method to clean up the
// channels.
func NewQueue(setters ...JobQueueSetter) (JobQueue, error) {
	newJobQueue := &jobQueue{
		stages:   make([]*stage, 0),
		finished: make(chan struct{}),
	}
	for _, setter := range setters {
		err := setter(newJobQueue)
		if err != nil {
			return nil, err
		}
	}
	if newJobQueue.output == nil {
		newJobQueue.output = make(chan interface{})
		if newJobQueue.sump {
			go func(c <-chan interface{}) {
				for _ = range c {
					// drop it
				}
			}(newJobQueue.output)
		}
	} else if newJobQueue.sump {
		return nil, fmt.Errorf("ought not have sump and output channel")
	}

	// NOTE: go in reverse to tie stage channels together
	nextOutput := newJobQueue.output
	nextFinished := newJobQueue.finished
	for si := len(newJobQueue.stages) - 1; si >= 0; si-- {
		thisInput := make(chan interface{})
		thisTerminate := make(chan struct{})

		stg := newJobQueue.stages[si]
		stg.input = thisInput
		stg.terminate = thisTerminate
		stg.output = nextOutput
		stg.finished = nextFinished

		nextOutput = thisInput
		nextFinished = thisTerminate
	}
	// spawn up the stage monitors
	for _, stg := range newJobQueue.stages {
		go stageMonitor(stg)
	}

	newJobQueue.input = nextOutput
	newJobQueue.terminate = nextFinished
	return newJobQueue, nil
}

// Enqueue adds a job to the tail of the job queue.
func (q *jobQueue) Enqueue(datum interface{}) {
	q.input <- datum
}

// Dequeue takes the next completed job off the queue.
func (q *jobQueue) Dequeue() interface{} {
	return <-q.output
}

// Input returns the queue's input channel.
func (q *jobQueue) Input() chan<- interface{} {
	return q.input
}

// Output returns the queue's output channel.
func (q *jobQueue) Output() <-chan interface{} {
	return q.output
}

// Quit method is called by the client once the queue is no longer
// needed, to clean up the channels. The Quit method closes the client
// output channel specified when the queue was created.
func (q *jobQueue) Quit() {
	if len(q.stages) > 0 {
		q.terminate <- struct{}{}
		<-q.finished
		for _, stg := range q.stages {
			close(stg.input)
		}
	}
	close(q.output)
}

type JobQueueSetter func(*jobQueue) error

// OutputSump creates a queue that discards jobs after
// completetion. This is used when one of the job stages performs a
// desired side effect.
func OutputSump() JobQueueSetter {
	return func(q *jobQueue) error {
		q.sump = true
		return nil
	}
}

func Output(out chan interface{}) JobQueueSetter {
	return func(q *jobQueue) error {
		if out == nil {
			return fmt.Errorf("channel ought be valid")
		}
		q.output = out
		return nil
	}
}
