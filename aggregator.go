package aggregator

import (
	"sync"
	"sync/atomic"
	"time"
)

type Aggregator[K comparable, T any] struct {
	Processor    func([]K, ...any) (map[K]T, error)
	Timeout      time.Duration
	MaxQueryOnce int
	Args         []any

	notify  chan notifyObject[K, T]
	running int32
}

type notifyObject[K comparable, T any] struct {
	id K
	ch chan Result[T]
}

func NewAggregator[K comparable, T any](processor func([]K, ...any) (map[K]T, error), timeout time.Duration, maxQueryOnce int) *Aggregator[K, T] {
	return &Aggregator[K, T]{
		Processor:    processor,
		Timeout:      timeout,
		MaxQueryOnce: maxQueryOnce,

		notify: make(chan notifyObject[K, T]),
	}
}

func (a *Aggregator[K, T]) Run() {
	if atomic.CompareAndSwapInt32(&a.running, 0, 1) {
		println("run!")
		go a.process()
	}
}

func (a *Aggregator[K, T]) process() {
	t := time.NewTimer(a.Timeout)
	for {
		// wait first notification
		data := <-a.notify

		fetchList := map[K][]chan Result[T]{
			data.id: {data.ch},
		}

		// timer, see: https://kknews.cc/zh-hk/news/rlp9ypn.html
		if !t.Stop() && len(t.C) > 0 {
			<-t.C
		}
		t.Reset(a.Timeout)

		println("[start] in:", data.id, "waiting...")

		// wait other notification
	wait:
		for {
			select {
			case data := <-a.notify:
				fetchList[data.id] = append(fetchList[data.id], data.ch)
				if len(fetchList) >= a.MaxQueryOnce {
					// reach max query
					break wait
				}
			case <-t.C:
				// timeout
				break wait
			}
		}

		keys := make([]K, len(fetchList))
		i := 0
		for k := range fetchList {
			keys[i] = k
			i++
		}

		// execute processor
		results, err := a.Processor(keys, a.Args...)

		// return results
		for i, queue := range fetchList {
			var result Result[T]

			if err != nil {
				result.Error = err
			} else if r, ok := results[i]; ok {
				result.Value = r
			} else {
				result.Error = ErrNoResult
			}

			// send result
			for _, c := range queue {
				go func(c chan Result[T]) {
					c <- result
					close(c)
				}(c)
			}
		}
	}
}

func (a *Aggregator[K, T]) QueryChan(key K) <-chan Result[T] {
	c := make(chan Result[T], 1)
	a.notify <- notifyObject[K, T]{
		id: key,
		ch: c,
	}
	return c
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
