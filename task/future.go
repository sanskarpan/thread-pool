// Package task provides Future/Promise pattern for async results
package task

import (
	"context"
	"sync"
	"time"
)

// Future represents a future result of an asynchronous operation
type Future struct {
	result     interface{}
	err        error
	done       chan struct{}
	once       sync.Once
	mu         sync.Mutex
	dependents []*Task
}

// NewFuture creates a new Future
func NewFuture() *Future {
	return &Future{
		done: make(chan struct{}),
	}
}

// SetResult sets the result and marks the future as complete
func (f *Future) SetResult(result interface{}) {
	f.once.Do(func() {
		f.result = result
		close(f.done)
		f.notifyDependents()
	})
}

// SetError sets an error and marks the future as complete
func (f *Future) SetError(err error) {
	f.once.Do(func() {
		f.err = err
		close(f.done)
		f.notifyDependents()
	})
}

// OnComplete registers a task to be notified when this future is resolved.
func (f *Future) OnComplete(dependent *Task) {
	f.mu.Lock()
	if f.IsDone() {
		f.mu.Unlock()
		dependent.resolveDependency()
		return
	}
	f.dependents = append(f.dependents, dependent)
	f.mu.Unlock()
}

func (f *Future) notifyDependents() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, dependent := range f.dependents {
		dependent.resolveDependency()
	}
	// Clear dependents after notification
	f.dependents = nil
}

// Get waits for the result and returns it
func (f *Future) Get() (interface{}, error) {
	<-f.done
	return f.result, f.err
}

// GetWithTimeout waits for the result with a timeout
func (f *Future) GetWithTimeout(timeout time.Duration) (interface{}, error) {
	select {
	case <-f.done:
		return f.result, f.err
	case <-time.After(timeout):
		return nil, context.DeadlineExceeded
	}
}

// GetWithContext waits for the result with a context
func (f *Future) GetWithContext(ctx context.Context) (interface{}, error) {
	select {
	case <-f.done:
		return f.result, f.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// IsDone returns true if the future is complete
func (f *Future) IsDone() bool {
	select {
	case <-f.done:
		return true
	default:
		return false
	}
}

// Then executes a callback when the future completes successfully
func (f *Future) Then(callback func(interface{})) *Future {
	next := NewFuture()

	go func() {
		result, err := f.Get()
		if err != nil {
			next.SetError(err)
			return
		}

		callback(result)
		next.SetResult(result)
	}()

	return next
}

// Catch executes a callback when the future completes with an error
func (f *Future) Catch(callback func(error)) *Future {
	next := NewFuture()

	go func() {
		result, err := f.Get()
		if err != nil {
			callback(err)
			next.SetError(err)
			return
		}

		next.SetResult(result)
	}()

	return next
}

// Finally executes a callback when the future completes (success or error)
func (f *Future) Finally(callback func()) {
	go func() {
		<-f.done
		callback()
	}()
}

// All waits for all futures to complete
func All(futures ...*Future) *Future {
	result := NewFuture()

	go func() {
		results := make([]interface{}, len(futures))

		for i, f := range futures {
			res, err := f.Get()
			if err != nil {
				result.SetError(err)
				return
			}
			results[i] = res
		}

		result.SetResult(results)
	}()

	return result
}

// Any returns the first future to complete successfully
func Any(futures ...*Future) *Future {
	result := NewFuture()

	for _, f := range futures {
		go func(future *Future) {
			res, err := future.Get()
			if err == nil {
				result.SetResult(res)
			}
		}(f)
	}

	return result
}

// Race returns the first future to complete (success or error)
func Race(futures ...*Future) *Future {
	result := NewFuture()

	for _, f := range futures {
		go func(future *Future) {
			res, err := future.Get()
			if err != nil {
				result.SetError(err)
			} else {
				result.SetResult(res)
			}
		}(f)
	}

	return result
}
