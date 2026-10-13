package main

import (
	"context"
	"errors"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestUnexpectedConsumerTermination(t *testing.T) {
	for _, cause := range []error{nil, errors.New("database unavailable")} {
		done := make(chan error, 1)
		done <- cause
		err := waitForWorker(make(chan os.Signal), done, func() {}, time.Second)
		if err == nil {
			t.Fatal("unexpected termination must fail the process")
		}
		if cause != nil && !errors.Is(err, cause) {
			t.Fatalf("lost failure: %v", err)
		}
	}
}

func TestSignalCancelsAndDrainsWorker(t *testing.T) {
	signals := make(chan os.Signal, 1)
	signals <- syscall.SIGTERM
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { <-ctx.Done(); done <- nil }()
	if err := waitForWorker(signals, done, cancel, time.Second); err != nil {
		t.Fatal(err)
	}
	if ctx.Err() == nil {
		t.Fatal("worker was not canceled")
	}
}

func TestDrainTimeoutFails(t *testing.T) {
	signals := make(chan os.Signal, 1)
	signals <- syscall.SIGTERM
	if err := waitForWorker(signals, make(chan error), func() {}, time.Millisecond); err == nil {
		t.Fatal("expected timeout failure")
	}
}
