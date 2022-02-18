package aggregator

import (
	"testing"
	"time"
)

func TestAggregatorList(t *testing.T) {
	aggrList := NewAggregatorList(
		NewAggregator(func(k []string, a ...any) (map[string]string, error) {
			return map[string]string{
				"key1": "val1",
				"key2": "val2",
				"key3": "val3",
			}, nil
		}, 10*time.Millisecond, 10, 1),
		NewAggregator(func(k []string, a ...any) (map[string]string, error) {
			return map[string]string{
				"key1": "val1",
				"key2": "val2",
				"key3": "val3",
				"key4": "val4",
			}, nil
		}, 100*time.Millisecond, 10, 1),
		NewAggregator(func(k []string, a ...any) (map[string]string, error) {
			return map[string]string{
				"key4": "val4",
				"key5": "val5",
			}, nil
		}, 100*time.Millisecond, 10, 1),
	)

	assertEqual(t, aggrList.QueryValue("key1"), "val1")
	assertEqual(t, aggrList.QueryValue("key2"), "val2")
	assertEqual(t, aggrList.QueryValue("key3"), "val3")
	assertEqual(t, aggrList.QueryValue("key4"), "val4")
	assertEqual(t, aggrList.QueryValue("key5"), "val5")
	assertEqual(t, aggrList.Query("key6").Error, ErrNoResult)
}
