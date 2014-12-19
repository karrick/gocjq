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

func TestJobQueueWithoutJobQueueStages(t *testing.T) {
	out := make(chan interface{})
	queue, err := NewQueue(out)
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Quit()

	go func() {
		pre := &silly{}
		queue.Enqueue(pre)
	}()

	v := queue.Dequeue()
	val := v.(*silly)
	if val.err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", val.err, nil)
	}
}

func TestJobQueueStageInvalidWorkerCount(t *testing.T) {
	out := make(chan interface{})
	_, err := NewQueue(out, Stage(0, "Foo"))
	if err == nil || !strings.Contains(err.Error(), "ought to have at least one worker") {
		t.Errorf("Actual: %#v; Expected: %#v", err, "ought to have at least one worker")
	}

	_, err = NewQueue(out, Stage(-1, "Foo"))
	if err == nil || !strings.Contains(err.Error(), "ought to have at least one worker") {
		t.Errorf("Actual: %#v; Expected: %#v", err, "ought to have at least one worker")
	}
}

func TestJobQueueStagesInvokedInProperOrder(t *testing.T) {
	out := make(chan interface{})
	queue, err := NewQueue(out, Stage(3, "Add"), Stage(5, "Multiply"))
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
