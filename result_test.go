package aggregator

import "testing"

func TestResult(t *testing.T) {
	t.Run("result get value", func(t *testing.T) {
		r := Result[string]{Value: "foo"}
		v, err := r.Get()
		assertEqual(t, v, "foo")
		assertEqual(t, err == nil, true)
	})

	t.Run("result get error", func(t *testing.T) {
		r := Result[string]{Error: NoResult}
		v, err := r.Get()
		assertEqual(t, v, "")
		assertEqual(t, err == NoResult, true)
	})
}

func TestNoResult(t *testing.T) {
	nr := newNoResult[string]()
	assertEqual(t, nr.Error == NoResult, true)

	_, err := nr.Get()
	assertEqual(t, err == NoResult, true)

	assertEqual(t, nr.IsNoResult(), true)
}
