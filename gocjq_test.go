package gocjq

import (
	"strings"
	"testing"
)

////////////////////////////////////////

type someJobType struct {
	a, b int
	err  error
}

func (self *someJobType) Add() {
	self.a += self.b
}

func (self *someJobType) Multiply() {
	self.a *= self.b
}

var (
	result int
)

func (self *someJobType) Result() {
	result = self.a
}

func TestOutputNil(t *testing.T) {
	_, err := NewQueue(Output(nil))
	expected := "channel ought be valid"
	if err == nil || !strings.Contains(err.Error(), expected) {
		t.Errorf("Actual: %#v; Expected: %#v", err, expected)
	}
}

func TestOutputSumpWithQueueFails(t *testing.T) {
	_, err := NewQueue(OutputSump(), Output(make(chan interface{})))
	expected := "ought not have sump and output channel"
	if err == nil || !strings.Contains(err.Error(), expected) {
		t.Errorf("Actual: %#v; Expected: %#v", err, expected)
	}
}

func TestWithoutStages(t *testing.T) {
	queue, err := NewQueue()
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Quit()

	go func() {
		queue.Enqueue(&someJobType{a: 5})
	}()

	v := queue.Dequeue()
	val := v.(*someJobType)
	if val.a != 5 {
		t.Errorf("Actual: %#v; Expected: %#v", val.a, 5)
	}
}

func TestStageInvalidWorkerCount(t *testing.T) {
	_, err := NewQueue(Stage(Min(0), Method("Foo")))
	if err == nil || !strings.Contains(err.Error(), "ought to have at least one worker") {
		t.Errorf("Actual: %#v; Expected: %#v", err, "ought to have at least one worker")
	}

	_, err = NewQueue(Stage(Min(-1), Method("Foo")))
	if err == nil || !strings.Contains(err.Error(), "ought to have at least one worker") {
		t.Errorf("Actual: %#v; Expected: %#v", err, "ought to have at least one worker")
	}
}

func TestStagesInvokedInProperOrderUsingEnqueueAndDequeue(t *testing.T) {
	queue, err := NewQueue(
		Stage(Min(20), Method("Add")),
		Stage(Min(1000), Method("Multiply")))
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Quit()

	go func() {
		queue.Enqueue(&someJobType{a: 13, b: 42})
	}()

	v := queue.Dequeue()
	val := v.(*someJobType)
	if val.a != (13+42)*42 {
		t.Errorf("Actual: %#v; Expected: %#v", val.a, (13+42)*42)
	}
	if val.err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", val.err, nil)
	}
}

func TestStagesInvokedInProperOrderUsingInputAndOutput(t *testing.T) {
	queue, err := NewQueue(
		Stage(Min(2), Method("Add")),
		Stage(Min(1), Method("Multiply")))
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Quit()

	go func() {
		queue.Input() <- &someJobType{a: 13, b: 42}
	}()

	v := <-queue.Output()
	val := v.(*someJobType)
	if val.a != (13+42)*42 {
		t.Errorf("Actual: %#v; Expected: %#v", val.a, (13+42)*42)
	}
	if val.err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", val.err, nil)
	}
}

func TestOutputSump(t *testing.T) {
	queue, err := NewQueue(
		Stage(Min(3), Method("Add")),
		Stage(Min(5), Method("Result")),
		OutputSump())
	if err != nil {
		t.Fatal(err)
	}

	jobSent := make(chan struct{})
	result = 0
	go func() {
		queue.Input() <- &someJobType{a: 13, b: 42}
		jobSent <- struct{}{}
	}()

	<-jobSent
	queue.Quit()
	if result != 55 {
		t.Errorf("Actual: %#v; Expected: %#v", result, 55)
	}
}
