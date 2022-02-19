package aggregator

import (
	"sync"
)

// AggregatorList is a type defintion of []*Aggregator[K, T].
type AggregatorList[K comparable, T any] []*Aggregator[K, T]

// NewList creates a new slice of Aggregators.
// When an aggregator returns a NoResult error
// it will call the next aggregator of the AggregatorList by order.
func NewList[K comparable, T any](aggregators ...*Aggregator[K, T]) AggregatorList[K, T] {
	return aggregators
}

// Run all aggregators of the AggregatorList by order.
func (aggregators AggregatorList[K, T]) Run() AggregatorList[K, T] {
	for _, a := range aggregators {
		a.Run()
	}
	return aggregators
}

// Query result in aggregators of the AggregatorList by order.
func (aggregators AggregatorList[K, T]) Query(key K) Result[T] {
	for i, a := range aggregators {
		result := a.Query(key)
		if result.Error != nil {
			if i == len(aggregators)-1 {
				return result
			}
			if result.IsNoResult() {
				continue
			} else {
				return result
			}
		}
		return result
	}
	return newNoResult[T]()
}

// It is a shortcut for Query(key).Get()
func (aggregators AggregatorList[K, T]) QueryResult(key K) (T, error) {
	return aggregators.Query(key).Get()
}

// It is a shortcut for Query(key).Value
func (aggregators AggregatorList[K, T]) QueryValue(key K) T {
	return aggregators.Query(key).Value
}

// Query result in aggregator of the AggregatorList by order
// with multiple keys and return a slice of Result[T] synchronously.
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
