package aggregator

import "fmt"

var ErrNoResult = fmt.Errorf("no result")

type Result[T any] struct {
	Value T
	Error error
}

func (r Result[T]) Get() (T, error) {
	return r.Value, r.Error
}

func ResultEmpty[T any]() Result[T] {
	var r Result[T]
	r.Error = ErrNoResult
	return r
}
