package aggregator

import (
	"sync"
	"time"
)

type Aggregator[K comparable, T any] struct {
	Processor    func([]K, ...any) (map[K]T, error)
	Timeout      time.Duration
	MaxQueryOnce int
	Args         []any

	flushChan chan map[K][]chan Result[T]
	notify    chan notifyObject[K, T]
}

type notifyObject[K comparable, T any] struct {
	id K
	ch chan Result[T]
}

func NewAggregator[K comparable, T any](processor func([]K, ...any) (map[K]T, error), timeout time.Duration, maxQueryOnce int, workerSize int) *Aggregator[K, T] {
	if workerSize < 1 {
		workerSize = 1
	}

	a := &Aggregator[K, T]{
		Processor:    processor,
		Timeout:      timeout,
		MaxQueryOnce: maxQueryOnce,

		flushChan: make(chan map[K][]chan Result[T], workerSize),
		notify:    make(chan notifyObject[K, T]),
	}

	// start flush workers
	for i := 0; i < workerSize; i++ {
		go a.flushWorker(a.flushChan)
	}

	// start aggregator
	go a.run()

	return a
}

func (a *Aggregator[K, T]) run() {
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

		if a.MaxQueryOnce > 1 {
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
		}

		a.flushChan <- fetchList
	}
}

func (a *Aggregator[K, T]) flushWorker(fetchChan <-chan map[K][]chan Result[T]) {
	for fetchList := range fetchChan {
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
