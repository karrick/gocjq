package gocjq

import (
	"fmt"
	"log"
	"reflect"
	"time"
)

const (
	defaultWorkerIdleTimeout = time.Minute / 4
	defaultMaxToMinRatio     = 16
)

type stage struct {
	methodName                                 string
	input                                      chan interface{}
	output                                     chan<- interface{}
	terminate                                  <-chan struct{}
	finished                                   chan<- struct{}
	workerIdle, terminateWorker, workerExitted chan struct{}
	workerCount, workerMin, workerMax          int
	busyHook                                   BusyHook
	idleHook                                   IdleHook
	idleTimeout, busyTimeout                   time.Duration
}

type BusyHook func(min, count, max int) int
type IdleHook func(min, count, max int) int

// Stage is a job queue configuration function that appends a job
// stage to the newly created job queue. The client specifies the name
// of the method to invoke on the job type, and optionally specifies
// the minimum and maximum number of workers that should work on this
// stage. The default minimum is 1, and the default maximum is 16
// times the actual minimum.
func Stage(setters ...StageSetter) JobQueueSetter {
	return func(q *jobQueue) error {
		if q.input != nil {
			return fmt.Errorf("stage cannot be created after queue")
		}
		stg := &stage{
			workerMin:   1,
			idleTimeout: defaultWorkerIdleTimeout,
			busyTimeout: defaultWorkerIdleTimeout * 2,
		}
		for _, setter := range setters {
			err := setter(stg)
			if err != nil {
				return err
			}
		}
		if stg.busyTimeout <= stg.idleTimeout {
			return fmt.Errorf("stage busy timeout must be larger than its idle timeout")
		}
		if stg.workerMax == 0 {
			stg.workerMax = defaultMaxToMinRatio * stg.workerMin
		}
		if stg.workerMax < stg.workerMin {
			return fmt.Errorf("stage minimum workers ought to be less than or equal to maximum workers")
		}
		if stg.busyHook == nil {
			log.Printf("setting busy hook to default of add one worker when busy")
			stg.busyHook = func(_, _, _ int) int {
				return 1
			}
		}
		if stg.idleHook == nil {
			log.Printf("setting idle hook to default of remove one worker when idle")
			stg.busyHook = func(_, _, _ int) int {
				return 1
			}
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
			log.Printf("[VERBOSE] %s idle", stg.methodName)
			numberToRemove := stg.idleHook(stg.workerMin, stg.workerCount, stg.workerMax)
			switch {
			case numberToRemove > 0:
				if stg.workerCount-numberToRemove < stg.workerMin {
					log.Printf("[WARNING] %s idle; idle hook returned number which would cause worker count to fall below minimum: %d - %d < %d", stg.methodName, stg.workerCount, numberToRemove, stg.workerMin)
					numberToRemove = stg.workerCount - stg.workerMin
				}
				for index := 0; index < numberToRemove; index++ {
					stg.terminateWorker <- struct{}{}
				}
			case numberToRemove < 0:
				log.Printf("[WARNING] %s idle; idle hook returned negative number: %d", stg.methodName, numberToRemove)
			}
		case <-time.After(stg.busyTimeout):
			log.Printf("[VERBOSE] %s busy", stg.methodName)
			numberToAdd := stg.busyHook(stg.workerMin, stg.workerCount, stg.workerMax)
			switch {
			case numberToAdd > 0:
				if stg.workerCount+numberToAdd > stg.workerMax {
					log.Printf("[WARNING] %s busy; busy hook returned number which would cause worker count to rise above maximum: %d + %d > %d", stg.methodName, stg.workerCount, numberToAdd, stg.workerMax)
					numberToAdd = stg.workerMax - stg.workerCount
				}
				if numberToAdd > 0 {
					spawn(stg, numberToAdd)
				}
			case numberToAdd < 0:
				log.Printf("[WARNING] %s busy; busy hook returned negative number: %d", stg.methodName, numberToAdd)
			}
		case <-stg.terminate:
			// log.Printf("[DEBUG] monitor terminate: %s", stg.methodName)
			// NOTE: count backwards to prevent workerCount race
			for index := stg.workerCount; index > 0; index-- {
				// log.Printf("[DEBUG] terminating: %s %d", stg.methodName, index)
				stg.terminateWorker <- struct{}{}
			}
		}
		log.Printf("[DEBUG] monitor %s has %d workers", stg.methodName, stg.workerCount)
	}
	// log.Print("[DEBUG] monitor finished: ", stg.methodName)
	stg.finished <- struct{}{}
}

func spawn(stg *stage, count int) {
	log.Printf("[DEBUG] %s worker spawn: %d workers", stg.methodName, count)
	for index := 0; index < count; index++ {
		go worker(stg, stg.terminateWorker, stg.workerExitted)
	}
	stg.workerCount += count
}

func worker(stg *stage, terminate <-chan struct{}, finished chan<- struct{}) {
	var input <-chan interface{} = stg.input // narrowing cast

workerLoop:
	for {
		select {
		case datum := <-input:
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
			break workerLoop
		case <-time.After(stg.idleTimeout):
			stg.workerIdle <- struct{}{}
		}
	}
	// log.Print("[VERBOSE] worker finished: ", stg.methodName)
	finished <- struct{}{}
}

// StageSetter type defines functions that modify a newly created job
// stage with various configuration settings. Each stage must have a
// method, and optionally one or both of the Min and Max number of
// workers set. Each stage's configuration is independent of the
// configuration for other stages. In other words, one stage may have
// a minimum of 10 and a maximum of 20 workers, but another stage may
// have a minimum of 50 and a maximum of 200 workers.
type StageSetter func(*stage) error

// Method is a stage configuration function that specifies the name of
// the method to be invoked on the job structure to be processed.
func Method(name string) StageSetter {
	return func(stg *stage) error {
		if name == "" {
			return fmt.Errorf("Method ought be a non-empty string")
		}
		stg.methodName = name
		return nil
	}
}

// Max is a stage configuration function that specifies the maximum
// number of workers to be simultaneously processing jobs for the
// respective job stage. The default is to have one worker for a
// stage.
func Max(count int) StageSetter {
	return func(stg *stage) error {
		if count <= 0 {
			return fmt.Errorf("Max ought be greater than 0: %d", count)
		}
		stg.workerMax = count
		return nil
	}
}

// Min is a stage configuration function that specifies the minimum
// number of workers to be simultaneously processing jobs for the
// respective job stage. The default is to allow up to 16 times the
// actual minimum number of workers for a stage.
func Min(count int) StageSetter {
	return func(stg *stage) error {
		if count <= 0 {
			return fmt.Errorf("ought to have at least one worker: %d", count)
		}
		stg.workerMin = count
		return nil
	}
}

// Static is a stage configuration function that specifies the static
// number of workers a stage ought to have.
func Static(count int) StageSetter {
	return func(stg *stage) error {
		if count <= 0 {
			return fmt.Errorf("ought to have at least one worker: %d", count)
		}
		stg.workerMin = count
		stg.workerMax = count
		return nil
	}
}

// Busy is a stage configuration function that specifies what BusyHook
// function to invoke when workers are busy.
func Busy(hook BusyHook) StageSetter {
	return func(stg *stage) error {
		stg.busyHook = hook
		return nil
	}
}

// Idle is a stage configuration function that specifies what IdleHook
// function to invoke when workers are idle.
func Idle(hook IdleHook) StageSetter {
	return func(stg *stage) error {
		stg.idleHook = hook
		return nil
	}
}

// BusyTimeout is a stage configuration function that specifies how
// long before a stage monitor considers that stage busy.
func BusyTimeout(t time.Duration) StageSetter {
	return func(stg *stage) error {
		stg.busyTimeout = t
		return nil
	}
}

// IdleTimeout is a stage configuration function that specifies how
// long before a stage worker considers itself idle.
func IdleTimeout(t time.Duration) StageSetter {
	return func(stg *stage) error {
		stg.idleTimeout = t
		return nil
	}
}
