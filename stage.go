package gocjq

import (
	"fmt"
	// "log"
	"reflect"
	"time"
)

const (
	workerIdleTimeout = time.Minute
)

type stage struct {
	input                             chan interface{}
	output                            chan<- interface{}
	methodName                        string
	terminate                         <-chan struct{}
	finished                          chan<- struct{}
	workerCount, workerMin, workerMax int

	workerIdle, terminateWorker, workerExitted chan struct{}
}

// Stage is used during job queue creation time to append a job stage
// to the job queue. The client specifies the name of the method to
// invoke on the job type, and optionally specifies the minimum and
// maximum number of workers that should work on this stage. The
// default minimum is 1, and the default maximum is 16 times the
// actual minimum.
func Stage(setters ...StageSetter) JobQueueSetter {
	return func(q *jobQueue) error {
		if q.input != nil {
			return fmt.Errorf("stage cannot be created after queue")
		}
		stg := &stage{workerMin: 1}
		for _, setter := range setters {
			err := setter(stg)
			if err != nil {
				return err
			}
		}
		if stg.workerMax == 0 {
			stg.workerMax = 16 * stg.workerMin
		}
		if stg.workerMax < stg.workerMin {
			return fmt.Errorf("stage minimum workers ought to be less than or equal to maximum workers")
		}
		q.stages = append(q.stages, stg)
		return nil
	}
}

func stageMonitor(stg *stage) {
	// log.Print("[DEBUG] monitor spawn: ", stg.methodName)

	// NOTE: input and output channels, methodName, workerMin, and
	// workerMax already set
	stg.workerIdle = make(chan struct{})
	stg.terminateWorker = make(chan struct{})
	stg.workerExitted = make(chan struct{})

	// spawn first workers
	spawn(stg, stg.workerMin)
	stg.workerCount = stg.workerMin

monitorLoop:
	for {
		select {
		case <-stg.workerExitted:
			// log.Printf("[DEBUG] worker exitted: %s", stg.methodName)
			stg.workerCount--
			if stg.workerCount <= 0 {
				break monitorLoop
			}
		case <-stg.workerIdle:
			if stg.workerCount > stg.workerMin {
				// log.Print("[DEBUG] %s idle", stg.methodName)
				stg.terminateWorker <- struct{}{}
			} else {
				// log.Printf("[DEBUG] %s idle; at minimum number of workers: %d", stg.methodName, stg.workerMin)
			}
		case <-time.After(workerIdleTimeout * 2):
			// no workers idle after a minute; double what
			// we have, not to exceed max
			additional := stg.workerCount
			if stg.workerCount+additional > stg.workerMax {
				// log.Printf("[DEBUG] %s busy; at maximum number of workers: %d", stg.methodName, stg.workerMax)
				additional = stg.workerMax - stg.workerCount
			}
			if additional > 0 {
				// log.Printf("[DEBUG] %s busy", stg.methodName)
				spawn(stg, additional)
				stg.workerCount += additional
			}
		case <-stg.terminate:
			// log.Printf("[DEBUG] monitor terminate: %s", stg.methodName)
			// NOTE: count backwards to prevent workerCount race
			for index := stg.workerCount; index > 0; index-- {
				// log.Printf("[DEBUG] terminating: %s %d", stg.methodName, index)
				stg.terminateWorker <- struct{}{}
			}
		}
		// log.Printf("[DEBUG] monitor %s has %d workers", stg.methodName, stg.workerCount)
	}
	// log.Print("[DEBUG] monitor finished: ", stg.methodName)
	stg.finished <- struct{}{}
}

func spawn(stg *stage, count int) {
	// log.Printf("[DEBUG] %s worker spawn: %d workers", stg.methodName, count)
	for index := 0; index < count; index++ {
		go worker(stg, stg.terminateWorker, stg.workerExitted)
	}
}

func worker(stg *stage, terminate <-chan struct{}, finished chan<- struct{}) {
	var input <-chan interface{} = stg.input // narrowing cast

workerLoop:
	for {
		select {
		case datum := <-input:
			// log.Print("[DEBUG] worker job: ", stg.methodName)
			datumType := reflect.TypeOf(datum)
			method, ok := datumType.MethodByName(stg.methodName)
			if !ok {
				panic(fmt.Errorf("%T has no method %v", datum, stg.methodName))
			}
			values := make([]reflect.Value, 1)
			values[0] = reflect.ValueOf(datum)
			method.Func.Call(values)
			stg.output <- datum
		case <-terminate:
			// log.Print("[DEBUG] worker terminate: ", stg.methodName)
			break workerLoop
		case <-time.After(time.Minute):
			// log.Print("[DEBUG] worker idle: ", stg.methodName)
			stg.workerIdle <- struct{}{}
		}
	}
	// log.Print("[DEBUG] worker finished: ", stg.methodName)
	finished <- struct{}{}
}

type StageSetter func(*stage) error

func Method(name string) StageSetter {
	return func(stg *stage) error {
		if name == "" {
			return fmt.Errorf("Method ought be a non-empty string")
		}
		stg.methodName = name
		return nil
	}
}

func Max(count int) StageSetter {
	return func(stg *stage) error {
		if count <= 0 {
			return fmt.Errorf("Max ought be greater than 0: %d", count)
		}
		stg.workerMax = count
		return nil
	}
}

func Min(count int) StageSetter {
	return func(stg *stage) error {
		if count <= 0 {
			return fmt.Errorf("ought to have at least one worker: %d", count)
		}
		stg.workerMin = count
		return nil
	}
}
