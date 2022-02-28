package aggregator

import (
	"fmt"
	"testing"
	"time"
)

func TestList(t *testing.T) {
	aggrList := NewList(
		New(func(k []string) (map[string]string, error) {
			fmt.Println("list0", k)
			return map[string]string{
				"key1": "val1",
				"key2": "val2",
				"key3": "val3",
			}, nil
		}, 10*time.Millisecond, 10),
		New(func(k []string) (map[string]string, error) {
			fmt.Println("list1", k)
			return map[string]string{
				"key1": "val1",
				"key2": "val2",
				"key3": "val3",
				"key4": "val4",
			}, nil
		}, 50*time.Millisecond, 10),
		New(func(k []string) (map[string]string, error) {
			fmt.Println("list2", k)
			return map[string]string{
				"key4": "val4",
				"key5": "val5",
			}, nil
		}, 50*time.Millisecond, 10),
	).Run()

	t.Run("Query", func(t *testing.T) {
		t.Parallel()
		assertEqual(t, aggrList.QueryValue("key1"), "val1")
		assertEqual(t, aggrList.QueryValue("key4"), "val4")
		assertEqual(t, aggrList.QueryValue("key5"), "val5")
	})

	t.Run("Query error", func(t *testing.T) {
		t.Parallel()
		assertEqual(t, aggrList.Query("key6").Error == NoResult, true)
	})

	t.Run("QueryMulti", func(t *testing.T) {
		t.Parallel()
		results := aggrList.QueryMulti([]string{"key1", "key2", "key3", "key4", "key5"})
		for i, result := range results { // avoid using reflect.DeepEqual with errors
			assertEqual(t, result.Value, fmt.Sprintf("val%d", i+1))
		}
	})
}
