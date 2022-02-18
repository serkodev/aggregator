package aggregator

import (
	"sync"
	"time"
)

type Aggregator[K comparable, T any] struct {
	Processor    func([]K) (map[K]T, error)
	Timeout      time.Duration
	MaxQueryOnce int

	flushChan  chan map[K][]chan Result[T]
	notifyChan chan notifyObject[K, T]
}

type notifyObject[K comparable, T any] struct {
	key K
	ch  chan Result[T]
}

// maxQueryOnce: 0 unlimit
// timeout: only wait max query once if < 0
func NewAggregator[K comparable, T any](processor func([]K) (map[K]T, error), timeout time.Duration, maxQueryOnce int, workerSize int) *Aggregator[K, T] {
	if workerSize < 1 {
		workerSize = 1
	}

	a := &Aggregator[K, T]{
		Processor:    processor,
		Timeout:      timeout,
		MaxQueryOnce: maxQueryOnce,

		flushChan:  make(chan map[K][]chan Result[T], workerSize),
		notifyChan: make(chan notifyObject[K, T]),
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
		data := <-a.notifyChan
		fetchList := map[K][]chan Result[T]{
			data.key: {data.ch},
		}

		// clear timer
		if !t.Stop() && len(t.C) > 0 {
			<-t.C
		}

		if a.MaxQueryOnce > 1 { // TODO: a.MaxQueryOnce 0
			t.Reset(a.Timeout)

			// wait other notification
		wait:
			for {
				select {
				case data := <-a.notifyChan:
					println("data in")
					fetchList[data.key] = append(fetchList[data.key], data.ch)
					if a.MaxQueryOnce > 0 && len(fetchList) >= a.MaxQueryOnce {
						println("max query flush")
						// reach max query
						break wait
					}
				case <-t.C:
					println("timeout flush")
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
		results, err := a.Processor(keys)

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
	ch := make(chan Result[T], 1)
	a.notifyChan <- notifyObject[K, T]{
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
