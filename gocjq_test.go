package gocjq

import (
	"strings"
	"testing"
)

////////////////////////////////////////

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

var (
	result int
)

func (self *silly) Result() {
	result = self.a
}

func TestJobQueueOutputNil(t *testing.T) {
	_, err := NewQueue(Output(nil))
	expected := "channel ought be valid"
	if err == nil || !strings.Contains(err.Error(), expected) {
		t.Errorf("Actual: %#v; Expected: %#v", err, expected)
	}
}

func TestJobQueueOutputSumpWithQueueFails(t *testing.T) {
	_, err := NewQueue(OutputSump(), Output(make(chan interface{})))
	expected := "ought not have sump and output channel"
	if err == nil || !strings.Contains(err.Error(), expected) {
		t.Errorf("Actual: %#v; Expected: %#v", err, expected)
	}
}

func TestJobQueueWithoutJobQueueStages(t *testing.T) {
	queue, err := NewQueue()
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Quit()

	go func() {
		queue.Enqueue(&silly{})
	}()

	v := queue.Dequeue()
	val := v.(*silly)
	if val.err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", val.err, nil)
	}
}

func TestJobQueueStageInvalidWorkerCount(t *testing.T) {
	_, err := NewQueue(Stage(0, "Foo"))
	if err == nil || !strings.Contains(err.Error(), "ought to have at least one worker") {
		t.Errorf("Actual: %#v; Expected: %#v", err, "ought to have at least one worker")
	}

	_, err = NewQueue(Stage(-1, "Foo"))
	if err == nil || !strings.Contains(err.Error(), "ought to have at least one worker") {
		t.Errorf("Actual: %#v; Expected: %#v", err, "ought to have at least one worker")
	}
}

func TestJobQueueStagesInvokedInProperOrderUsingEnqueueAndDequeue(t *testing.T) {
	queue, err := NewQueue(Stage(3, "Add"), Stage(5, "Multiply"))
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Quit()

	go func() {
		pre := &silly{a: 13, b: 42}
		queue.Enqueue(pre)
	}()

	v := queue.Dequeue()
	val := v.(*silly)
	if val.a != (13+42)*42 {
		t.Errorf("Actual: %#v; Expected: %#v", val.a, (13+42)*42)
	}
	if val.err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", val.err, nil)
	}
}

func TestJobQueueStagesInvokedInProperOrderUsingInputAndOutput(t *testing.T) {
	queue, err := NewQueue(Stage(3, "Add"), Stage(5, "Multiply"))
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Quit()

	go func() {
		queue.Input() <- &silly{a: 13, b: 42}
	}()

	v := <-queue.Output()
	val := v.(*silly)
	if val.a != (13+42)*42 {
		t.Errorf("Actual: %#v; Expected: %#v", val.a, (13+42)*42)
	}
	if val.err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", val.err, nil)
	}
}

func TestJobQueueOutputSump(t *testing.T) {
	queue, err := NewQueue(Stage(3, "Add"), OutputSump(), Stage(5, "Result"))
	if err != nil {
		t.Fatal(err)
	}

	jobSent := make(chan struct{})
	result = 0
	go func() {
		queue.Input() <- &silly{a: 13, b: 42}
		jobSent <- struct{}{}
	}()

	<-jobSent
	queue.Quit()
	if result != 55 {
		t.Errorf("Actual: %#v; Expected: %#v", result, 55)
	}
}
