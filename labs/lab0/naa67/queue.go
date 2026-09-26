package lab0

import (
	"sync"
)

// Queue is a simple FIFO queue that is unbounded in size.
// Push may be called any number of times and is not
// expected to fail or overwrite existing entries.
type Queue[T any] struct {
	size int
	data []T
}

// NewQueue returns a new queue which is empty.
func NewQueue[T any]() *Queue[T] {
	return &Queue[T]{}
}

// Push adds an item to the end of the queue.
func (q *Queue[T]) Push(t T) {
	q.data = append(q.data, t)
	q.size += 1
}

// Pop removes an item from the beginning of the queue
// and returns it unless the queue is empty.
//
// If the queue is empty, returns the zero value for T and false.
//
// If you are unfamiliar with "zero values", consider revisiting
// this section of the Tour of Go: https://go.dev/tour/basics/12
func (q *Queue[T]) Pop() (T, bool) {
	var result T
	if q.size == 0 {
		return result, false
	} else {
		result, q.data = q.data[0], q.data[1:]
		q.size -= 1
		return result, true
	}
}

// ConcurrentQueue provides the same semantics as Queue but
// is safe to access from many goroutines at once.
//
// You can use your implementation of Queue[T] here.
//
// If you are stuck, consider revisiting this section of
// the Tour of Go: https://go.dev/tour/concurrency/9
type ConcurrentQueue[T any] struct {
	mtx   sync.Mutex
	queue *Queue[T]
}

func NewConcurrentQueue[T any]() *ConcurrentQueue[T] {
	return &ConcurrentQueue[T]{queue: NewQueue[T]()}
}

// Push adds an item to the end of the queue
func (q *ConcurrentQueue[T]) Push(t T) {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	q.queue.Push(t)
}

// Pop removes an item from the beginning of the queue.
// Returns a zero value and false if empty.
func (q *ConcurrentQueue[T]) Pop() (T, bool) {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	return q.queue.Pop()
}
