// It is often convenient to partition a job into multiple steps, and
// have them run concurrently, like a car factory might have different
// stages in a manufactoring facility.
//
// It is also convenient to specify how many workers are desired at each
// stage of the process. For one stage you may need 5 workers, but for
// another stage you may want between 25 and 50.
//
// This library, go concurent job queues, gocjq, makes setting up
// these sort of examples quite easy. Create a structure to hold your
// job's information, give your structure methods to be invoked for
// each stage, then create a JobQueue that invokes your methods in the
// proper order.
package gocjq

import (
	"fmt"
)

// JobQueue allows enqueuing and dequeueing of jobs. The caller MUST
// invoke Quit method when the queue is no longer needed.
type JobQueue interface {
	Input() chan<- interface{}
	Output() <-chan interface{}
	Quit()
}

type jobQueue struct {
	input, output       chan interface{}
	stages              []*stage
	terminate, finished chan struct{}
}

// NewQueue creates a job queue. New jobs are enqueued by sending a
// job to the channel returned by the queue's Input method. Jobs are
// processed by the specified job stages, then sent to the queue's
// output channel. If no output channel is specified, one will be
// created by NewQueue. If no output channel is desired, the
// OutputSump method is used to create a drain that will loop over the
// queue's output channel after completion. Once the queue is no
// longer needed, the client MUST call the Quit method to clean up the
// channels.
//
//  type someJob struct {
//      a, b int
//      err  error
//  }
//
//  func (self *someJob) Add() {
//      if err == nil {
//          self.a += self.b
//      }
//  }
//
//  func (self *someJob) Divide() {
//      if err == nil {
//          if self.b != 0 {
//              self.err = fmt.Errorf("divide by zero")
//          } else {
//              self.a /= self.b
//          }
//      }
//  }
//
//  func main() {
//      queue, err := gocjq.NewQueue(
//          gocjq.Stage(gocjq.Method("Divide"), gocjq.Min(4)),
//          gocjq.Stage(gocjq.Method("Add")))
//      if err != nil {
//          log.Fatal(err)
//      }
//      defer queue.Quit()
//
//      go func() {
//          queue.Input() <- &someJob{a: 13, b: 42}
//      }()
//
//      result := <- queue.Output()
//      if result.err != nil {
//          fmt.Println("job error: ", result.err)
//      } else {
//          fmt.Println("job result: ", result.a)
//      }
//  }
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

// Input returns the queue's input channel.
func (q *jobQueue) Input() chan<- interface{} {
	return q.input
}

// Output returns the queue's output channel.
func (q *jobQueue) Output() <-chan interface{} {
	return q.output
}

// Quit is called by the client once the queue is no longer needed to
// clean up the channels. The Quit method closes the output channel,
// including when the client specifies an output channel using the
// Output method durng queue creation.
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

// JobQueueSetter type defines functions that modify a newly created
// job queue with various configuration settings.
type JobQueueSetter func(*jobQueue) error

// Output is a job queue configuration function that allows the client
// to specify a channel to send the completed jobs to. Failing to
// receive jobs from this channel will have the effect of halting the
// job queue once all the channels are full.
func Output(out chan interface{}) JobQueueSetter {
	return func(q *jobQueue) error {
		if q.output != nil {
			return fmt.Errorf("output channel already specified; was a sump created?")
		}
		if out == nil {
			return fmt.Errorf("channel ought be valid")
		}
		q.output = out
		return nil
	}
}

// OutputSump is a job queue configuration function that creates a
// queue that discards jobs after completetion. This is used when one
// of the job stages performs a desired side effect. Because completed
// jobs are discarded by the library, there is no need for client
// removal of completed jobs from the job queue.
//
//  func main() {
//      queue, err := gocjq.NewQueue(
//          gocjq.Stage(gocjq.Method("Divide"), gocjq.Min(4)),
//          gocjq.Stage(gocjq.Method("Add")),
//          gocjq.Stage(gocjq.Method("Print"), gocjq.Min(2)),
//          gocjq.OutputSump())
//      if err != nil {
//          log.Fatal(err)
//      }
//      defer queue.Quit()
//      input := queue.Input()
//
//      jobSent := make(chan struct{})
//      go func() {
//          input <- &someJob{a: 13, b: 42}
//          jobSent <- struct{}{}
//      }()
//
//      <-jobSent
//      queue.Quit()
//  }
func OutputSump() JobQueueSetter {
	return func(q *jobQueue) error {
		if q.output != nil {
			return fmt.Errorf("output channel already specified")
		}
		q.output = make(chan interface{})
		go func(c <-chan interface{}) {
			for _ = range c {
				// drop it
			}
		}(q.output)
		return nil
	}
}
