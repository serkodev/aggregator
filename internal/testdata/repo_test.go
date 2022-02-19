package testdata

import (
	"fmt"
	"sync"
	"testing"
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

// test case
var cacheRepo *UserCacheRepo
var repo *UserRepo

func setup() {
	cacheRepo = NewUserCacheRepo()
	repo = NewUserRepo(cacheRepo)

	repo.db.Store("u1", "user1")
	repo.db.Store("u2", "user2")
	repo.db.Store("u3", "user3")
	repo.db.Store("u4", "user4")
	repo.db.Store("u5", "user5")
}

func TestRepo(t *testing.T) {
	setup()

	repo.GetByID("u1")
	repo.GetByID("u1")
	repo.GetByID("u1")
}
