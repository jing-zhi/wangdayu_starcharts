package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/apex/log"
	"golang.org/x/sync/errgroup"
)

var (
	errNoMorePages  = errors.New("no more pages to get")
	ErrTooManyStars = errors.New("repo has too many stargazers, github won't allow us to list all stars")
)

// 特定时间内的一颗星星
type Stargazer struct {
	StarredAt time.Time `json:"starred_at"`
}

// 返回 所有时间段的星
func (gh *GitHub) Stargazers(ctx context.Context, repo Repository) (stars []Stargazer, err error) {
	sem := make(chan bool, 4)

	if gh.totalPages(repo) > 400 {
		return stars, ErrTooManyStars
	}

	var g errgroup.Group
	var lock sync.Mutex
	for page := 1; page <= gh.lastPage(repo); page++ {
		sem <- true
		page := page
		g.Go(func() error {
			defer func() { <-sem }()
			result, err := gh.getStargazersPage(ctx, repo, page)
			if errors.Is(err, errNoMorePages) {
				return nil
			}
			if err != nil {
				return err
			}
			lock.Lock()
			defer lock.Unlock()
			stars = append(stars, result...)
			return nil
		})
	}
	err = g.Wait()
	sort.Slice(stars, func(i, j int) bool {
		return stars[i].StarredAt.Before(stars[j].StarredAt)
	})
	return
}

func (gh *GitHub) getStargazersPage(ctx context.Context, repo Repository, page int) ([]Stargazer, error) {
	log := log.WithField("repo", repo.FullName).WithField("page", page)
	defer log.Trace("get page").Stop(nil)

	var stars []Stargazer
	key := fmt.Sprintf("%s_%d", repo.FullName, page)

	var etag string

	resp, err := gh.makeStarPageRequest(ctx, repo, page, etag)
	if err != nil {
		return stars, err
	}

	bts, err := io.ReadAll(resp.Body)
	if err != nil {
		return stars, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNotModified:
		effectiveEtags.Inc()
		log.Info("not modified")

		if err != nil {
			log.WithError(err).Warnf("failed to get %s from cache", key)

			return gh.getStargazersPage(ctx, repo, page)
		}
		return stars, err
	case http.StatusForbidden:
		rateLimits.Inc()
		log.Warn("rate limit hit")
		return stars, ErrRateLimit
	case http.StatusOK:
		if err := json.Unmarshal(bts, &stars); err != nil {
			return stars, err
		}
		if len(stars) == 0 {
			return stars, errNoMorePages
		}

		etag = resp.Header.Get("etag")
		
		return stars, nil
	default:
		return stars, fmt.Errorf("%w: %v", ErrGitHubAPI, string(bts))
	}
}

func (gh *GitHub) totalPages(repo Repository) int {
	return repo.StargazersCount / gh.pageSize
}

func (gh *GitHub) lastPage(repo Repository) int {
	return gh.totalPages(repo) + 1
}

func (gh *GitHub) makeStarPageRequest(ctx context.Context, repo Repository, page int, etag string) (*http.Response, error) {
	url := fmt.Sprintf(
		"https://api.github.com/repos/%s/stargazers?page=%d&per_page=%d",
		repo.FullName,
		page,
		gh.pageSize,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Accept", "application/vnd.github.v3.star+json")
	if etag != "" {
		req.Header.Add("If-None-Match", etag)
	}

	return gh.authorizedDo(req, 0)
}