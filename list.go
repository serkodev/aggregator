package aggregator

import (
	"sync"
)

type AggregatorList[K comparable, T any] []*Aggregator[K, T]

func NewList[K comparable, T any](aggregators ...*Aggregator[K, T]) AggregatorList[K, T] {
	return aggregators
}

func (aggregators AggregatorList[K, T]) Run() AggregatorList[K, T] {
	for _, a := range aggregators {
		a.Run()
	}
	return aggregators
}

func (aggregators AggregatorList[K, T]) Query(key K) Result[T] {
	for i, a := range aggregators {
		result := a.Query(key)
		if result.Error != nil {
			if i == len(aggregators)-1 {
				return result
			}
			continue
		}
		return result
	}
	return newNoResult[T]()
}

func (aggregators AggregatorList[K, T]) QueryResult(key K) (T, error) {
	r := aggregators.Query(key)
	return r.Value, r.Error
}

func (aggregators AggregatorList[K, T]) QueryValue(key K) T {
	return aggregators.Query(key).Value
}

func (aggregators AggregatorList[K, T]) QueryMulti(keys []K) []Result[T] {
	output := make([]Result[T], len(keys))
	var w sync.WaitGroup
	w.Add(len(keys))
	for i, key := range keys {
		go func(i int, key K) {
			defer w.Done()
			output[i] = aggregators.Query(key)
		}(i, key)
	}
	w.Wait()
	return output
}
