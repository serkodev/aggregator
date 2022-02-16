package testdata

import (
	"testing"
)

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
