package lab0_test

import (
	"context"
	"strconv"
	"testing"

	"cs426.cloud/lab0"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func chanToSlice[T any](ch chan T) []T {
	vals := make([]T, 0)
	for item := range ch {
		vals = append(vals, item)
	}
	return vals
}

type mergeFunc = func(chan string, chan string, chan string)

func runMergeTest(t *testing.T, merge mergeFunc) {
	t.Run("empty channels", func(t *testing.T) {
		a := make(chan string)
		b := make(chan string)
		out := make(chan string)
		close(a)
		close(b)

		merge(a, b, out)
		// If your lab0 hangs here, make sure you are closing your channels!
		require.Empty(t, chanToSlice(out))
	})

	// Please write your own tests
}

func TestMergeChannels(t *testing.T) {
	runMergeTest(t, func(a, b, out chan string) {
		lab0.MergeChannels(a, b, out)
	})
}

func TestMergeOrCancel(t *testing.T) {
	runMergeTest(t, func(a, b, out chan string) {
		_ = lab0.MergeChannelsOrCancel(context.Background(), a, b, out)
	})

	t.Run("already canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		a := make(chan string, 1)
		b := make(chan string, 1)
		out := make(chan string, 10)

		eg, _ := errgroup.WithContext(context.Background())
		eg.Go(func() error {
			return lab0.MergeChannelsOrCancel(ctx, a, b, out)
		})
		err := eg.Wait()
		a <- "a"
		b <- "b"

		require.Error(t, err)
		require.Equal(t, []string{}, chanToSlice(out))
	})

	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		a := make(chan string)
		b := make(chan string)
		out := make(chan string, 10)

		eg, _ := errgroup.WithContext(context.Background())
		eg.Go(func() error {
			return lab0.MergeChannelsOrCancel(ctx, a, b, out)
		})
		a <- "a"
		b <- "b"
		cancel()

		err := eg.Wait()
		require.Error(t, err)
		require.Equal(t, []string{"a", "b"}, chanToSlice(out))
	})
}

type channelFetcher struct {
	ch chan string
}

func newChannelFetcher(ch chan string) *channelFetcher {
	return &channelFetcher{ch: ch}
}

func (f *channelFetcher) Fetch() (string, bool) {
	v, ok := <-f.ch
	return v, ok
}

func TestMergeFetches(t *testing.T) {
	runMergeTest(t, func(a, b, out chan string) {
		lab0.MergeFetches(newChannelFetcher(a), newChannelFetcher(b), out)
	})
}

func TestMergeFetchesAdditional(t *testing.T) {
	t.Run("one side empty", func(t *testing.T) {
		a := make(chan string, 3)
		b := make(chan string)
		out := make(chan string, 10)
		a <- "a1"
		a <- "a2"
		a <- "a3"
		close(a)
		close(b)

		lab0.MergeFetches(newChannelFetcher(a), newChannelFetcher(b), out)
		require.Equal(t, []string{"a1", "a2", "a3"}, chanToSlice(out))
	})

	t.Run("both sides", func(t *testing.T) {
		a := make(chan string, 2)
		b := make(chan string, 2)
		out := make(chan string, 10)
		a <- "a1"
		a <- "a2"
		b <- "b1"
		b <- "b2"
		close(a)
		close(b)

		lab0.MergeFetches(newChannelFetcher(a), newChannelFetcher(b), out)
		require.ElementsMatch(t, []string{"a1", "a2", "b1", "b2"}, chanToSlice(out))
	})

	t.Run("unbuffered out", func(t *testing.T) {
		a := make(chan string, 2)
		b := make(chan string, 2)
		out := make(chan string)
		a <- "a1"
		a <- "a2"
		b <- "b1"
		b <- "b2"
		close(a)
		close(b)

		go lab0.MergeFetches(newChannelFetcher(a), newChannelFetcher(b), out)
		require.ElementsMatch(t, []string{"a1", "a2", "b1", "b2"}, chanToSlice(out))
	})

	t.Run("stress test", func(t *testing.T) {
		N := 10000
		expected_a := make([]string, N)
		expected_b := make([]string, N)
		for i := 0; i < N; i++ {
			expected_a[i] = "a" + strconv.Itoa(i)
			expected_b[i] = "b" + strconv.Itoa(i)
		}

		a := make(chan string)
		b := make(chan string)
		out := make(chan string)

		go func() {
			for _, v := range expected_a {
				a <- v
			}
			close(a)
		}()
		go func() {
			for _, v := range expected_b {
				b <- v
			}
			close(b)
		}()

		go lab0.MergeFetches(newChannelFetcher(a), newChannelFetcher(b), out)

		var as []string
		var bs []string
		for v := range out {
			if v[0] == 'a' {
				as = append(as, v)
			} else {
				bs = append(bs, v)
			}
		}
		require.Equal(t, expected_a, as)
		require.Equal(t, expected_b, bs)
	})
}
