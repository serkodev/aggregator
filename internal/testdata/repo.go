package testdata

import (
	"fmt"
	"sync"
	"time"

	"github.com/serkodev/aggregator"
)

type UserRepo struct {
	db             sync.Map
	aggregatorList aggregator.AggregatorList[string, string]
	aggregator     *aggregator.Aggregator[string, string]
	cacheRepo      *UserCacheRepo
}

func NewUserRepo(cache *UserCacheRepo) *UserRepo {
	r := &UserRepo{
		cacheRepo: cache,
	}
	r.aggregator = aggregator.NewAggregator(r.Processor, 1000*time.Millisecond, 3600)
	r.aggregator.Run()

	r.aggregatorList = aggregator.NewAggregatorList(cache.aggregator, r.aggregator)
	r.aggregatorList.Run()
	return r
}

func (repo *UserRepo) GetByID(userID string) (string, error) {
	return repo.aggregatorList.Query(userID).Get()
}

func (repo *UserRepo) Processor(keys []string, _ ...any) (map[string]string, error) {
	fmt.Println("load from user repo")
	result := make(map[string]string)
	for _, key := range keys {
		if val, ok := repo.db.Load(key); ok {
			// cache
			repo.cacheRepo.db.Store(key, val)

			result[key] = val.(string)
		}
	}
	return result, nil
}

// cache repo

type UserCacheRepo struct {
	db         sync.Map
	aggregator *aggregator.Aggregator[string, string]
}

func NewUserCacheRepo() *UserCacheRepo {
	r := &UserCacheRepo{}
	r.aggregator = aggregator.NewAggregator(r.Processor, 1000*time.Millisecond, 3600)
	r.aggregator.Run()
	return r
}

func (repo *UserCacheRepo) Processor(keys []string, _ ...any) (map[string]string, error) {
	fmt.Println("load from user cache")
	result := make(map[string]string)
	for _, key := range keys {
		if val, ok := repo.db.Load(key); ok {
			result[key] = val.(string)
		}
	}
	return result, nil
}
