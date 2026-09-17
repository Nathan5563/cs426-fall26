package lab0_test

import (
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cs426.cloud/lab0"
	"github.com/stretchr/testify/require"
	// "golang.org/x/sync/errgroup"
)

type MockFetcher struct {
	data  []string
	index int
	mu    sync.Mutex

	delay time.Duration
	// Int32 is an atomic int32
	activeFetches atomic.Int32
}

func NewMockFetcher(data []string, delay time.Duration) *MockFetcher {
	return &MockFetcher{
		data:  data,
		delay: delay,
	}
}

func (f *MockFetcher) Fetch() (string, bool) {
	f.activeFetches.Add(1)
	defer f.activeFetches.Add(-1)

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.index >= len(f.data) {
		return "", false
	}
	f.index++

	// Don't hold the lock while simulating the delay
	f.mu.Unlock()
	// add random jitter to delay
	time.Sleep(f.delay + time.Duration(rand.Intn(10))*time.Millisecond)
	f.mu.Lock()

	return f.data[f.index-1], true
}

func (f *MockFetcher) ActiveFetches() int32 {
	return f.activeFetches.Load()
}

func sliceToMap(slice []string) map[string]bool {
	m := make(map[string]bool)
	for _, v := range slice {
		m[v] = true
	}
	return m
}

func checkResultSet(t *testing.T, expected []string, actual []string) {
	expectedMap := sliceToMap(expected)
	actualMap := sliceToMap(actual)
	require.Equal(t, len(expectedMap), len(actualMap))
}

func callFetchNTimes(pf *lab0.ParallelFetcher, n int) []string {
	actual := make([]string, n)
	for i := 0; i < n; i++ {
		v, ok := pf.Fetch()
		if !ok {
			break
		}
		actual[i] = v
	}
	return actual
}

func TestParallelFetcher(t *testing.T) {
	t.Run("fetch basic", func(t *testing.T) {
		data := []string{"a", "b", "c", "d", "e"}
		pf := lab0.NewParallelFetcher(NewMockFetcher(data, 0), 1)

		results := callFetchNTimes(pf, 5)
		checkResultSet(t, data, results)

		// next call returns false
		_, ok := pf.Fetch()
		require.False(t, ok)
	})
	t.Run("fetch concurrency limits", func(t *testing.T) {
		N := 100
		data := make([]string, N)
		for i := 0; i < N; i++ {
			data[i] = strconv.Itoa(i)
		}
		mf := NewMockFetcher(data, 10*time.Millisecond)
		pf := lab0.NewParallelFetcher(mf, 3)

		done := make(chan struct{})
		go func() {
			for {
				select {
				case <-done:
					return
				default:
					require.LessOrEqual(t, mf.ActiveFetches(), int32(3))
				}
			}
		}()

		wg := sync.WaitGroup{}
		for i := 0; i < N; i++ {
			wg.Add(1)
			go func() {
				v, ok := pf.Fetch()
				require.True(t, ok)
				require.NotEmpty(t, v)
				wg.Done()
			}()
		}
		wg.Wait()

		// next call returns false
		_, ok := pf.Fetch()
		require.False(t, ok)

		done <- struct{}{}
	})
}

type barrierFetcher struct {
	wg sync.WaitGroup
}

func (f *barrierFetcher) Fetch() (string, bool) {
	f.wg.Done()
	f.wg.Wait()
	return "", true
}

func TestParallelFetcherAdditional(t *testing.T) {
	t.Run("empty fetcher", func(t *testing.T) {
		pf := lab0.NewParallelFetcher(NewMockFetcher([]string{}, 0), 2)

		_, ok := pf.Fetch()
		require.False(t, ok)
		_, ok = pf.Fetch()
		require.False(t, ok)
	})

	t.Run("reaches limit", func(t *testing.T) {
		limit := 10
		bf := &barrierFetcher{}
		bf.wg.Add(limit)
		pf := lab0.NewParallelFetcher(bf, limit)

		done := make(chan struct{})
		go func() {
			var wg sync.WaitGroup
			for i := 0; i < limit; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					pf.Fetch()
				}()
			}
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("fewer than limit concurrent fetches")
		}
	})

	t.Run("stress test", func(t *testing.T) {
		N := 1000
		workers := 100
		limit := 10
		data := make([]string, N)
		for i := 0; i < N; i++ {
			data[i] = strconv.Itoa(i)
		}
		mf := NewMockFetcher(data, 0)
		pf := lab0.NewParallelFetcher(mf, limit)

		var mu sync.Mutex
		recvd := make([]string, 0, N)
		var maxActive atomic.Int32
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				for {
					v, ok := pf.Fetch()
					if !ok {
						return
					}

					a := mf.ActiveFetches()
					if a > maxActive.Load() {
						maxActive.Store(a)
					}

					mu.Lock()
					recvd = append(recvd, v)
					mu.Unlock()
				}
			}()
		}
		wg.Wait()

		require.ElementsMatch(t, data, recvd)
		require.LessOrEqual(t, maxActive.Load(), int32(limit))

		_, ok := pf.Fetch()
		require.False(t, ok)
	})
}
