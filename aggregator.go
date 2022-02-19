package aggregator

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

type task[K comparable, T any] struct {
	key K
	ch  chan Result[T]
}

type Aggregator[K comparable, T any] struct {
	MaxWait time.Duration
	MaxSize int

	fn     func([]K) (map[K]T, error)
	ch     chan task[K, T]
	once   sync.Once
	debugf func(format string, v ...interface{})
}

var NeverFlushTimeout time.Duration = -1

// flushMaxQuery: 0 unlimit
// flushTimeout: only wait max query once if < 0
func New[K comparable, T any](fn func([]K) (map[K]T, error), flushMaxWait time.Duration, flushMaxSize int) *Aggregator[K, T] {
	a := &Aggregator[K, T]{
		MaxWait: flushMaxWait,
		MaxSize: flushMaxSize,
		fn:      fn,
		ch:      make(chan task[K, T]),
		debugf:  func(format string, v ...interface{}) {},
	}
	return a
}

func (a *Aggregator[K, T]) Debug() *Aggregator[K, T] {
	a.debugf = log.New(os.Stdout, fmt.Sprintf("[aggregator]"), 0).Printf
	return a
}

func (a *Aggregator[K, T]) Run() (*Aggregator[K, T], error) {
	return a.RunWithWorkers(1)
}

func (a *Aggregator[K, T]) RunWithWorkers(workers int) (*Aggregator[K, T], error) {
	if a.fn == nil {
		return a, errors.New("nil aggregator fn")
	}
	a.once.Do(func() {
		go a.run(workers)
	})
	return a, nil
}

func (a *Aggregator[K, T]) run(workers int) {
	// workers
	if workers < 1 {
		workers = 1
	}
	flushChan := make(chan map[K][]chan Result[T], workers)
	for i := 0; i < workers; i++ {
		go a.runWorker(flushChan)
	}

	// flush timer
	timer := time.NewTimer(a.MaxWait)
	for {
		// wait task to start
		task := <-a.ch
		a.debugf("[query] key start: %s", task.key)
		tasks := map[K][]chan Result[T]{
			task.key: {task.ch},
		}

		// stop timer
		if !timer.Stop() && len(timer.C) > 0 {
			<-timer.C
		}

		if a.MaxSize != 1 {
			if a.MaxWait != NeverFlushTimeout {
				// reset timer to count down
				timer.Reset(a.MaxWait)
			}
		wait:
			for {
				select {
				case task := <-a.ch:
					a.debugf("[query] key: %s", task.key)
					tasks[task.key] = append(tasks[task.key], task.ch)
					if a.MaxSize > 0 && len(tasks) >= a.MaxSize {
						a.debugf("[flush] max query reached")
						break wait
					}
				case <-timer.C:
					a.debugf("[flush] timeout")
					break wait
				}
			}
		}

		// flush
		flushChan <- tasks
	}
}

func (a *Aggregator[K, T]) runWorker(flushChan <-chan map[K][]chan Result[T]) {
	for tasks := range flushChan {
		keys := make([]K, 0, len(tasks))
		for k := range tasks {
			keys = append(keys, k)
		}

		// execute
		results, err := a.fn(keys)

		// return results
		for key, chans := range tasks {
			var result Result[T]

			if err != nil {
				result.Error = err
			} else if r, ok := results[key]; ok {
				result.Value = r
			} else {
				result.Error = NoResult
			}

			// send result to each task ch
			for _, ch := range chans {
				go func(ch chan Result[T]) {
					ch <- result
					close(ch)
				}(ch)
			}
		}
	}
}

func (a *Aggregator[K, T]) QueryChan(key K) <-chan Result[T] {
	ch := make(chan Result[T], 1)
	a.ch <- task[K, T]{
		key: key,
		ch:  ch,
	}
	return ch
}

func (a *Aggregator[K, T]) Query(key K) Result[T] {
	return <-a.QueryChan(key)
}

func (a *Aggregator[K, T]) QueryResult(key K) (T, error) {
	r := a.Query(key)
	return r.Value, r.Error
}

func (a *Aggregator[K, T]) QueryValue(key K) T {
	return a.Query(key).Value
}

func (a *Aggregator[K, T]) QueryMulti(keys []K) []Result[T] {
	output := make([]Result[T], len(keys))
	var w sync.WaitGroup
	w.Add(len(keys))
	for i, key := range keys {
		go func(i int, key K) {
			defer w.Done()
			output[i] = a.Query(key)
		}(i, key)
	}
	w.Wait()
	return output
}
