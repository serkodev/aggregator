package aggregator

import "errors"

var NoResult = errors.New("No result")

type Result[T any] struct {
	Value T
	Error error
}

func (r Result[T]) Get() (T, error) {
	return r.Value, r.Error
}

func (r Result[T]) IsNoResult() bool {
	return r.Error == NoResult
}

func newNoResult[T any]() Result[T] {
	return Result[T]{
		Error: NoResult,
	}
}
