gocjq
=====

concurrent job queue for go

### Description

It is often convenient to partition a job into multiple steps, and
have them run concurrently, like a car factory might have different
stages in a manufactoring facility.

It is also convenient to specify how many workers are desired at each
stage of the process. For one stage you may need 5 workers, but for
another stage you may want between 25 and 50.

This library, go concurent job queues, gocjq, makes setting up these
sort of examples quite easy. There are two interfaces, namely an
enqueue and dequeue fascility,

## Examples

### Error handling

In this example, it is important to ensure the job state does not have
an error prior to commencing the next stage. If the job already has an
error, then skip following steps.

```Go
type someJob struct {
	a, b float64
	err  error
}

func (self *someJob) Add() {
	if self.err == nil {
		self.a += self.b
	}
}

func (self *someJob) Divide() {
	if self.err == nil {
		if self.b != 0 {
			self.err = fmt.Errorf("divide by zero")
		} else {
			self.a /= self.b
		}
	}
}

func main() {
    queue, err := gocjq.NewQueue(
        gocjq.Stage(gocjq.Method("Divide"), gocjq.Min(4), gocjq.Max(64)),
        gocjq.Stage(gocjq.Method("Add"), gocjq.Min(32)),
        gocjq.Stage(gocjq.Method("Print"), gocjq.Min(2)))
    if err != nil {
        log.Fatal(err)
    }
    defer queue.Quit()
    input := queue.Input()

    go func() {
        input <- &someJob{a: 13, b: 42}
    }()

    v := <- queue.Output()
    val := v.(*someJob)

    if val.err != nil {
        log.Printf("[ERROR] Actual: %#v; Expected: %#v", val.err, nil)
    }
    if val.a != (13/42)+42 {
        log.Printf("[ERROR] Actual: %#v; Expected: %#v", val.a, (13/42)+42)
    }
}
```

### Creating a job sump

Sometimes there are no actions to take for a job after processing is
complete. This is often the case when the final stage of a job has
some sort of side effect, and the completed jobs can be discarded.

In this example, a job queue sump is created to drain completed jobs.

```Go
type someJob struct {
    a, b int
    err  error
}

func (self *someJob) Add() {
    if err == nil {
        self.a += self.b
    }
}

func (self *someJob) Divide() {
    if err == nil {
        if self.b != 0 {
            self.err = fmt.Errorf("divide by zero")
        } else {
            self.a /= self.b
        }
    }
}

func (self *someJob) Print() {
    if err == nil {
        fmt.Println("job result: ", self.a)
    } else {
        fmt.Println("job error: ", self.err)
    }
}

func main() {
    queue, err := gocjq.NewQueue(
        gocjq.Stage(gocjq.Method("Divide"), gocjq.Min(4)),
        gocjq.Stage(gocjq.Method("Add")),
        gocjq.Stage(gocjq.Method("Print"), gocjq.Min(2)),
        gocjq.OutputSump())
    if err != nil {
        log.Fatal(err)
    }
    defer queue.Quit()
    input := queue.Input()

    jobSent := make(chan struct{})
    go func() {
        input <- &someJob{a: 13, b: 42}
        jobSent <- struct{}{}
    }()

    <-jobSent
    queue.Quit()
}
```
