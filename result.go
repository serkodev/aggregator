package aggregator

import "errors"

var NoResult = errors.New("No result")

// Result of Aggregator query. Including Value and Error.
type Result[T any] struct {
	Value T
	Error error
}

// Value and Error in multiple return value
func (r Result[T]) Get() (T, error) {
	return r.Value, r.Error
}

// Use to check Result[T].Error equals NoResult.
// If fn of aggregator does not set the key of result map
// or returns an NoResult error, the Result[T].Error will be NoResult.
func (r Result[T]) IsNoResult() bool {
	return r.Error == NoResult
}

func newNoResult[T any]() Result[T] {
	return Result[T]{
		Error: NoResult,
	}
}
