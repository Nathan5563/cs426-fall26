package lab0

import (
	"context"
	"sync/atomic"
)

// Semaphore mirrors Go's library package `semaphore.Weighted`
// but with a smaller, simpler interface.
//
// Recall that a counting semaphore has two jobs:
//   - keep track of a number of available resources
//   - when resources are depleted, block waiters and resume them
//     when resources are available
type Semaphore struct {
	v      atomic.Int64
	signal chan struct{}
}

func NewSemaphore() *Semaphore {
	return &Semaphore{
		signal: make(chan struct{}, 1),
	}
}

// Post increments the semaphore value by one. If there are any
// callers waiting, it signals exactly one to wake up.
//
// Analagous to Release(1) in semaphore.Weighted. One important difference
// is that calling Release before any Acquire will panic in semaphore.Weighted,
// but calling Post() before Wait() should neither block nor panic in our interface.
func (s *Semaphore) Post() {
	if s.v.Load() == 0 {
		s.signal <- struct{}{}
	}
	s.v.Add(1)
}

// Wait decrements the semaphore value by one, if there are resources
// remaining from previous calls to Post. If there are no resources remaining,
// waits until resources are available or until the context is done, whichever
// is first.
//
// If the context is done with an error, returns that error. Returns `nil`
// in all other cases.
//
// Analagous to Acquire(ctx, 1) in semaphore.Weighted.
func (s *Semaphore) Wait(ctx context.Context) error {
	if s.v.Load() == 0 {
		select {
		case <-s.signal:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.v.Add(-1)
	return nil
}
