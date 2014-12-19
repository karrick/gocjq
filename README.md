gocjq
=====

concurrent job queue for go

### Example using input and output channels

```Go
type silly struct {
	a, b int
	err  error
}

func (self *silly) Add() {
	self.a += self.b
}

func (self *silly) Multiply() {
	self.a *= self.b
}

func main() {
	out := make(chan interface{})
	queue, err := gocjq.NewQueue(out, gocjq.Stage(3, "Add"), gocjq.Stage(5, "Multiply"))
	if err != nil {
	    log.Fatal(err)
	}
	defer queue.Quit()
    input := queue.Input()
    output := queue.Output()

	go func() {
	    input <- &silly{a: 13, b: 42}
	}()

	v := <- output
	val := v.(*silly)
	if val.a != (13+42)*42 {
	    log.Printf("[ERROR] Actual: %#v; Expected: %#v", val.a, (13+42)*42)
	}
	if val.err != nil {
		log.Printf("[ERROR] Actual: %#v; Expected: %#v", val.err, nil)
    }
}
```

### Example Using Enqueue() and Dequeue()

```Go
type silly struct {
	a, b int
	err  error
}

func (self *silly) Add() {
	self.a += self.b
}

func (self *silly) Multiply() {
	self.a *= self.b
}

func main() {
	out := make(chan interface{})
	queue, err := gocjq.NewQueue(out, gocjq.Stage(3, "Add"), gocjq.Stage(5, "Multiply"))
	if err != nil {
	    log.Fatal(err)
	}
	defer queue.Quit()

	go func() {
		queue.Enqueue(&silly{a: 13, b: 42})
	}()

	v := queue.Dequeue()
	val := v.(*silly)
	if val.a != (13+42)*42 {
	    log.Printf("[ERROR] Actual: %#v; Expected: %#v", val.a, (13+42)*42)
	}
	if val.err != nil {
		log.Printf("[ERROR] Actual: %#v; Expected: %#v", val.err, nil)
    }
}
```
