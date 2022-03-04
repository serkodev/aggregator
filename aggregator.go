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

// Aggregator is the instance, it contains the flush and worker settings.
// Create an instance of Aggregator, by using New()
type Aggregator[K comparable, T any] struct {
	// Max waiting time of worker to flush query tasks.
	MaxWait time.Duration

	// Max amount of query tasks, the worker will flush after reaching it.
	MaxSize int

	fn     func([]K) (map[K]T, error)
	ch     chan task[K, T]
	once   sync.Once
	debugf func(format string, v ...interface{})
}

// If NeverFlushTimeout is set for the flushMaxWait, the aggregator
// will never flush with timeout.
var NeverFlushTimeout time.Duration = -1

// New creates a new Aggregator. The flushMaxWait variable
// sets the maximum timeout of flushing. If flushMaxWait equals to
// NeverFlushTimeout then the aggregator will never flush with timeout.
// The flushMaxSize variable sets the maximum size of task.
// If the flushMaxSize <= 0, the aggregator will never flush with amount of tasks.
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

// If Debug() is called, the Aggregator will prints debug messages.
func (a *Aggregator[K, T]) Debug() *Aggregator[K, T] {
	a.debugf = log.New(os.Stdout, fmt.Sprintf("[aggregator]"), 0).Printf
	return a
}

// Start run Aggregator with single worker.
func (a *Aggregator[K, T]) Run() (*Aggregator[K, T], error) {
	return a.RunWithWorkers(1)
}

// Start run Aggregator with n workers.
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

		// stop timer, see: https://github.com/golang/go/issues/27169
		if !timer.Stop() {
			select {
			case <-timer.C: // drain from channel
			default:
			}
		}

		if a.MaxSize != 1 {
			if a.MaxWait != NeverFlushTimeout {
				// reset timer to count down
				timer.Reset(a.MaxWait)

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
			} else {
				for task := range a.ch {
					a.debugf("[query] key: %s", task.key)
					tasks[task.key] = append(tasks[task.key], task.ch)
					if a.MaxSize > 0 && len(tasks) >= a.MaxSize {
						a.debugf("[flush] max query reached")
						break
					}
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

// Query with a key and return with a Result[T] channel.
func (a *Aggregator[K, T]) QueryChan(key K) <-chan Result[T] {
	ch := make(chan Result[T], 1)
	a.ch <- task[K, T]{
		key: key,
		ch:  ch,
	}
	return ch
}

// Query with a key and return with a Result[T] synchronously.
// It is a shortcut for <-QueryChan(key)
func (a *Aggregator[K, T]) Query(key K) Result[T] {
	return <-a.QueryChan(key)
}

// Query with a key and return with Value and Error of Result[T] synchronously.
// It is a shortcut for Query(key).Get()
func (a *Aggregator[K, T]) QueryResult(key K) (T, error) {
	return a.Query(key).Get()
}

// Query with a key and return with Value Result[T] synchronously.
// It is a shortcut for Query(key).Value
func (a *Aggregator[K, T]) QueryValue(key K) T {
	return a.Query(key).Value
}

// Query with multiple keys and return a slice of Result[T] synchronously.
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
