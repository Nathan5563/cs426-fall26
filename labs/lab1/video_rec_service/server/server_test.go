package main

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	fipb "cs426.yale.edu/lab1/failure_injection/proto"
	"cs426.yale.edu/lab1/ranker"
	umc "cs426.yale.edu/lab1/user_service/mock_client"
	upb "cs426.yale.edu/lab1/user_service/proto"
	usl "cs426.yale.edu/lab1/user_service/server_lib"
	vmc "cs426.yale.edu/lab1/video_service/mock_client"
	vpb "cs426.yale.edu/lab1/video_service/proto"
	vsl "cs426.yale.edu/lab1/video_service/server_lib"

	pb "cs426.yale.edu/lab1/video_rec_service/proto"
	sl "cs426.yale.edu/lab1/video_rec_service/server_lib"
)

func TestServerBasic(t *testing.T) {
	vrOptions := sl.VideoRecServiceOptions{
		MaxBatchSize:    50,
		DisableFallback: true,
		DisableRetry:    true,
	}
	// You can specify failure injection options here or later send
	// SetInjectionConfigRequests using these mock clients
	uClient :=
		umc.MakeMockUserServiceClient(*usl.DefaultUserServiceOptions())
	vClient :=
		vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())
	vrService := sl.MakeVideoRecServiceServerWithMocks(
		vrOptions,
		uClient,
		vClient,
	)

	var userId uint64 = 204054
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := vrService.GetTopVideos(
		ctx,
		&pb.GetTopVideosRequest{UserId: userId, Limit: 5},
	)
	assert.True(t, err == nil)

	videos := out.Videos
	assert.Equal(t, 5, len(videos))
	assert.EqualValues(t, 1012, videos[0].VideoId)
	assert.Equal(t, "Harry Boehm", videos[1].Author)
	assert.EqualValues(t, 1209, videos[2].VideoId)
	assert.Equal(t, "https://video-data.localhost/blob/1309", videos[3].Url)
	assert.Equal(t, "precious here", videos[4].Title)

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	vClient.SetInjectionConfig(ctx, &fipb.SetInjectionConfigRequest{
		Config: &fipb.InjectionConfig{
			// fail one in 1 request, i.e., always fail
			FailureRate: 1,
		},
	})

	// Since we disabled retry and fallback, we expect the VideoRecService to
	// throw an error since the VideoService is "down".
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = vrService.GetTopVideos(
		ctx,
		&pb.GetTopVideosRequest{UserId: userId, Limit: 5},
	)
	assert.False(t, err == nil)
}

// ---

// test harness and helpers
//
// MakeHarness builds a VideoRecService with mock clients
// the harness exposes the service with methods h.topVideos() and h.stats()
//   and allows users to manipulate the upstream servers with h.injectUser()
//   and h.injectVideo()
// there are many helper methods here for ease of testing

const (
	goldenUser1 uint64 = 204054
	goldenUser2 uint64 = 203584
	reqTimeout         = 30 * time.Second
)

type harnessConfig struct {
	batchSize     int
	upstreamBatch int
	retry         bool
	fallback      bool
}

type harnessOption func(*harnessConfig)

func withBatch(n int) harnessOption {
	return func(c *harnessConfig) { c.batchSize = n }
}
func withUpstreamBatch(n int) harnessOption {
	return func(c *harnessConfig) { c.upstreamBatch = n }
}
func withRetry() harnessOption {
	return func(c *harnessConfig) { c.retry = true }
}
func withFallback() harnessOption {
	return func(c *harnessConfig) { c.fallback = true }
}

func MakeHarnessConfig(opts ...harnessOption) harnessConfig {
	cfg := harnessConfig{batchSize: 50, upstreamBatch: 50}
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

type harness struct {
	t           *testing.T
	cfg         harnessConfig
	recServer   *sl.VideoRecServiceServer
	userServer  *umc.MockUserServiceClient
	videoServer *vmc.MockVideoServiceClient
}

func MakeHarness(t *testing.T, opts ...harnessOption) *harness {
	t.Helper()
	cfg := MakeHarnessConfig(opts...)

	userOpts := usl.DefaultUserServiceOptions()
	userOpts.MaxBatchSize = cfg.upstreamBatch
	userServer := umc.MakeMockUserServiceClient(*userOpts)

	videoOpts := vsl.DefaultVideoServiceOptions()
	videoOpts.MaxBatchSize = cfg.upstreamBatch
	videoServer := vmc.MakeMockVideoServiceClient(*videoOpts)

	recOpts := sl.VideoRecServiceOptions{
		MaxBatchSize:    cfg.batchSize,
		DisableRetry:    !cfg.retry,
		DisableFallback: !cfg.fallback,
	}

	return &harness{
		t:   t,
		cfg: cfg,
		recServer: sl.MakeVideoRecServiceServerWithMocks(recOpts,
			userServer, videoServer),
		userServer:  userServer,
		videoServer: videoServer,
	}
}

func (h *harness) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), reqTimeout)
}

func (h *harness) topVideos(
	userId uint64,
	limit int32,
) (*pb.GetTopVideosResponse, error) {
	ctx, cancel := h.ctx()
	defer cancel()

	return h.recServer.GetTopVideos(ctx,
		&pb.GetTopVideosRequest{UserId: userId, Limit: limit})
}

func (h *harness) stats() *pb.GetStatsResponse {
	h.t.Helper()

	ctx, cancel := h.ctx()
	defer cancel()

	resp, err := h.recServer.GetStats(ctx, &pb.GetStatsRequest{})
	if err != nil {
		h.t.Fatal(err)
	}
	return resp
}

func (h *harness) injectUser(sleep time.Duration, failureRate int64) {
	h.t.Helper()

	ctx, cancel := h.ctx()
	defer cancel()

	if _, err := h.userServer.SetInjectionConfig(ctx,
		&fipb.SetInjectionConfigRequest{Config: &fipb.InjectionConfig{
			SleepNs:     sleep.Nanoseconds(),
			FailureRate: failureRate,
		}}); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) injectVideo(sleep time.Duration, failureRate int64) {
	h.t.Helper()

	ctx, cancel := h.ctx()
	defer cancel()

	if _, err := h.videoServer.SetInjectionConfig(ctx,
		&fipb.SetInjectionConfigRequest{Config: &fipb.InjectionConfig{
			SleepNs:     sleep.Nanoseconds(),
			FailureRate: failureRate,
		}}); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) expectedTopVideos(userId uint64, k int) []uint64 {
	h.t.Helper()

	me := h.getUsers([]uint64{userId})[0]
	seen := map[uint64]bool{}
	var liked []uint64
	for _, u := range h.getUsers(me.SubscribedTo) {
		for _, v := range u.LikedVideos {
			if !seen[v] {
				seen[v] = true
				liked = append(liked, v)
			}
		}
	}

	type scored struct{ id, score uint64 }
	var all []scored
	for _, v := range h.getVideos(liked) {
		all = append(all, scored{v.VideoId, rank(me, v)})
	}
	sort.SliceStable(all, func(i, j int) bool {
		return all[i].score > all[j].score
	})

	if k > 0 && k < len(all) {
		all = all[:k]
	}
	ids := make([]uint64, len(all))
	for i, s := range all {
		ids[i] = s.id
	}
	return ids
}

func (h *harness) getUsers(ids []uint64) []*upb.UserInfo {
	h.t.Helper()

	ctx, cancel := h.ctx()
	defer cancel()

	var out []*upb.UserInfo
	for start := 0; start < len(ids); start += h.cfg.upstreamBatch {
		end := min(start+h.cfg.upstreamBatch, len(ids))
		resp, err := h.userServer.GetUser(ctx,
			&upb.GetUserRequest{UserIds: ids[start:end]})
		if err != nil {
			h.t.Fatal(err)
		}
		out = append(out, resp.Users...)
	}
	return out
}

func (h *harness) getVideos(ids []uint64) []*vpb.VideoInfo {
	h.t.Helper()

	ctx, cancel := h.ctx()
	defer cancel()

	var out []*vpb.VideoInfo
	for start := 0; start < len(ids); start += h.cfg.upstreamBatch {
		end := min(start+h.cfg.upstreamBatch, len(ids))
		resp, err := h.videoServer.GetVideo(ctx,
			&vpb.GetVideoRequest{VideoIds: ids[start:end]})
		if err != nil {
			h.t.Fatal(err)
		}
		out = append(out, resp.Videos...)
	}
	return out
}

type counters struct {
	total, errors, active, userErrors, videoErrors, stale uint64
}

func assertCounters(t *testing.T, s *pb.GetStatsResponse, want counters) {
	t.Helper()

	got := counters{
		total:       s.TotalRequests,
		errors:      s.TotalErrors,
		active:      s.ActiveRequests,
		userErrors:  s.UserServiceErrors,
		videoErrors: s.VideoServiceErrors,
		stale:       s.StaleResponses,
	}
	assert.Equal(t, want, got)
}

func rank(user *upb.UserInfo, video *vpb.VideoInfo) uint64 {
	return (&ranker.BcryptRanker{}).Rank(
		user.UserCoefficients,
		video.VideoCoefficients,
	)
}

func videoIds(videos []*vpb.VideoInfo) []uint64 {
	ids := make([]uint64, len(videos))
	for i, v := range videos {
		ids[i] = v.VideoId
	}
	return ids
}

// ---

func TestLimitRespected(t *testing.T) {
	h := MakeHarness(t)

	all, err := h.topVideos(goldenUser1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(all.Videos) <= 5 {
		t.Fatalf("need more than 5 candidates, got %d", len(all.Videos))
	}

	five, err := h.topVideos(goldenUser1, 5)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, videoIds(all.Videos)[:5], videoIds(five.Videos),
		"recommendations keep the same order across limit sizes")

	huge, err := h.topVideos(goldenUser1, int32(len(all.Videos)+100))
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, videoIds(all.Videos), videoIds(huge.Videos),
		"limit larger than the candidate set returns everything")
}

func TestNoDuplicates(t *testing.T) {
	h := MakeHarness(t)

	resp, err := h.topVideos(goldenUser1, 0)
	if err != nil {
		t.Fatal(err)
	}

	seen := map[uint64]bool{}
	for _, v := range resp.Videos {
		assert.False(t, seen[v.VideoId], "video %d returned twice", v.VideoId)
		seen[v.VideoId] = true
	}
}

func TestDescendingScore(t *testing.T) {
	h := MakeHarness(t)

	resp, err := h.topVideos(goldenUser1, 0)
	if err != nil {
		t.Fatal(err)
	}

	me := h.getUsers([]uint64{goldenUser1})[0]
	prev := ^uint64(0)
	for _, v := range resp.Videos {
		score := rank(me, v)
		assert.LessOrEqual(t, score, prev, "video %d is out of order",
			v.VideoId)
		prev = score
	}
}

func TestBatchSizeRespected(t *testing.T) {
	ok := MakeHarness(t, withBatch(5), withUpstreamBatch(5))
	_, err := ok.topVideos(goldenUser1, 0)
	assert.NoError(t, err, "batches of exactly the upstream limit work")

	tooBig := MakeHarness(t, withBatch(6), withUpstreamBatch(5))
	_, err = tooBig.topVideos(goldenUser1, 0)
	assert.Equal(t, codes.InvalidArgument, status.Code(err),
		"batches bigger than the upstream limit are rejected")

	whole, err := MakeHarness(t).topVideos(goldenUser1, 0)
	if err != nil {
		t.Fatal(err)
	}
	split, err := MakeHarness(t, withBatch(1)).topVideos(goldenUser1, 0)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, videoIds(whole.Videos), videoIds(split.Videos),
		`responses from batched requests are the same as those from unbatched
		requests`)
}

func TestStatsWithRetry(t *testing.T) {
	cases := []struct {
		name     string
		opts     []harnessOption
		attempts uint64
	}{
		{"retry", []harnessOption{withRetry()}, 2},
		{"no retry", nil, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := MakeHarness(t, tc.opts...)
			h.injectVideo(0, 1)
			_, err := h.topVideos(goldenUser1, 5)
			assert.Equal(t, codes.Internal, status.Code(err),
				"upstream error code must be propagated")
			assertCounters(t, h.stats(),
				counters{total: 1, errors: 1, videoErrors: tc.attempts})
		})
	}
}

func TestStatsActive(t *testing.T) {
	h := MakeHarness(t)
	h.injectUser(time.Second, 0)

	done := make(chan error, 1)
	go func() {
		_, err := h.topVideos(goldenUser1, 5)
		done <- err
	}()

	giveUp := time.Now().Add(5 * time.Second)
	for h.stats().ActiveRequests == 0 && time.Now().Before(giveUp) {
		time.Sleep(10 * time.Millisecond)
	}

	assertCounters(t, h.stats(), counters{total: 1, active: 1})

	if err := <-done; err != nil {
		t.Fatal(err)
	}
	assertCounters(t, h.stats(), counters{total: 1})
}

func TestStatsLatency(t *testing.T) {
	h := MakeHarness(t)
	h.injectUser(time.Second, 0)

	if _, err := h.topVideos(goldenUser1, 5); err != nil {
		t.Fatal(err)
	}

	s := h.stats()
	assert.GreaterOrEqual(t, s.AverageLatencyMs, float32(1000))
	assert.GreaterOrEqual(t, s.P99LatencyMs, float32(1000))
	assert.InDelta(t, s.AverageLatencyMs, s.P99LatencyMs, 1,
		"average and p99 are equal in a single sample")
}

func TestUpstreamErrorAttribution(t *testing.T) {
	t.Run("unknown user", func(t *testing.T) {
		h := MakeHarness(t)
		_, err := h.topVideos(1, 5)
		assert.Equal(t, codes.NotFound, status.Code(err))
		assertCounters(t, h.stats(),
			counters{total: 1, errors: 1, userErrors: 1})
	})

	t.Run("user service down", func(t *testing.T) {
		h := MakeHarness(t, withRetry())
		if _, err := h.topVideos(goldenUser1, 5); err != nil {
			t.Fatal(err)
		}
		h.injectUser(0, 1)
		_, err := h.topVideos(goldenUser1, 5)
		assert.Equal(t, codes.Internal, status.Code(err))
		assertCounters(t, h.stats(),
			counters{total: 2, errors: 1, userErrors: 2})
	})
}

func TestConcurrentRequests(t *testing.T) {
	h := MakeHarness(t, withRetry())
	h.injectVideo(0, 20)

	const n = 40
	var wg sync.WaitGroup
	var failures atomic.Uint64
	for i := range n {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			if _, err := h.topVideos(goldenUser1+uint64(i), 5); err != nil {
				failures.Add(1)
			}
			h.stats()
		}(i)
	}
	wg.Wait()

	s := h.stats()
	assert.EqualValues(t, n, s.TotalRequests)
	assert.EqualValues(t, 0, s.ActiveRequests)
	assert.EqualValues(t, failures.Load(), s.TotalErrors)
}
