package gocjq

import (
	"fmt"
	"reflect"
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
	input, output chan interface{}
	stages        []jobStage
	sump          bool
	done          chan struct{}
}

type jobStage struct {
	howManyWorkers int
	methodName     string
}

// NewQueue creates a job queue. New jobs are enqueued with its
// Enqueue method, are processed by the specified job stages, then
// sent to the specified client output channel. Once the queue is no
// longer needed, the client MUST call the Quit method to clean up the
// channels.
func NewQueue(setters ...JobQueueSetter) (JobQueue, error) {
	newJobQueue := &jobQueue{
		stages: make([]jobStage, 0),
		done:   make(chan struct{}),
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
	output := newJobQueue.output

	// NOTE: go in reverse to tie stage channels together
	for si := len(newJobQueue.stages) - 1; si >= 0; si-- {
		input := make(chan interface{})

		// NOTE: must pass these into go routine for binding
		go func(si int, input, output chan interface{}) {
			thisStage := newJobQueue.stages[si]
			hmw := thisStage.howManyWorkers
			methodName := thisStage.methodName
			done := make(chan struct{})

			// spin off stage processors
			for index := 0; index < hmw; index++ {
				go func() {
					for datum := range input {
						datumType := reflect.TypeOf(datum)
						method, ok := datumType.MethodByName(methodName)
						if !ok {
							panic(fmt.Errorf("%T has no method %v", datum, methodName))
						}
						values := make([]reflect.Value, 1)
						values[0] = reflect.ValueOf(datum)
						method.Func.Call(values)
						output <- datum
					}
					done <- struct{}{}
				}()
			}
			// wait until all stage processors are complete
			for index := 0; index < hmw; index++ {
				<-done
			}
			close(output)
			if si == 0 {
				// the last stage to finish tells the queue that we're done
				newJobQueue.done <- struct{}{}
			}
		}(si, input, output)

		output = input
	}
	newJobQueue.input = output
	return newJobQueue, nil
}

// Enqueue adds a job to the tail of the job queue.
func (q *jobQueue) Enqueue(p interface{}) {
	q.input <- p
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
	close(q.input)
	if len(q.stages) > 0 {
		<-q.done
	}
}

type JobQueueSetter func(*jobQueue) error

// Stage is used during job queue creation time to append a job stage
// to the job queue. The client specifies the number of workers that
// should work on this stage, and the name of the method to invoke on
// the job type.
func Stage(howManyWorkers int, methodName string) JobQueueSetter {
	return func(q *jobQueue) error {
		if q.input != nil {
			return fmt.Errorf("stage cannot be created after queue")
		}
		if howManyWorkers <= 0 {
			return fmt.Errorf("stage ought to have at least one worker")
		}
		q.stages = append(q.stages, jobStage{howManyWorkers: howManyWorkers, methodName: methodName})
		return nil
	}
}

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
