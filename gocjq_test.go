package gocjq

import (
	"fmt"
	"strings"
	"testing"
)

////////////////////////////////////////

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

func (self *someJob) Multiply() {
	if self.err == nil {
		self.a *= self.b
	}
}

var (
	result float64
)

func (self *someJob) Result() {
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
	expected := "output channel already specified"
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
		queue.Input() <- &someJob{a: 5}
	}()

	v := <-queue.Output()
	val := v.(*someJob)
	if val.a != 5 {
		t.Errorf("Actual: %#v; Expected: %#v", val.a, 5)
	}
}

func TestStageInvalidWorkerCount(t *testing.T) {
	_, err := NewQueue(Stage(Method("Foo"), Min(0)))
	if err == nil || !strings.Contains(err.Error(), "ought to have at least one worker") {
		t.Errorf("Actual: %#v; Expected: %#v", err, "ought to have at least one worker")
	}

	_, err = NewQueue(Stage(Method("Foo"), Min(-1)))
	if err == nil || !strings.Contains(err.Error(), "ought to have at least one worker") {
		t.Errorf("Actual: %#v; Expected: %#v", err, "ought to have at least one worker")
	}
}

func TestStagesInvokedInProperOrderUsingInputAndOutput(t *testing.T) {
	queue, err := NewQueue(
		Stage(Method("Add"), Min(20)),
		Stage(Method("Multiply"), Min(100)))
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Quit()

	go func() {
		queue.Input() <- &someJob{a: 13, b: 42}
	}()

	v := <-queue.Output()
	val := v.(*someJob)
	if val.a != (13+42)*42 {
		t.Errorf("Actual: %#v; Expected: %#v", val.a, (13+42)*42)
	}
	if val.err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", val.err, nil)
	}
}

func TestOutputSump(t *testing.T) {
	queue, err := NewQueue(
		Stage(Method("Add"), Min(3)),
		Stage(Method("Result"), Min(5)),
		OutputSump())
	if err != nil {
		t.Fatal(err)
	}

	jobSent := make(chan struct{})
	result = 0
	go func() {
		queue.Input() <- &someJob{a: 13, b: 42}
		jobSent <- struct{}{}
	}()

	<-jobSent
	queue.Quit()
	if result != 55 {
		t.Errorf("Actual: %#v; Expected: %#v", result, 55)
	}
}
