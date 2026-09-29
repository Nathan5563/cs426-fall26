package server_lib

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"

	"cs426.yale.edu/lab1/ranker"
	umc "cs426.yale.edu/lab1/user_service/mock_client"
	upb "cs426.yale.edu/lab1/user_service/proto"
	pb "cs426.yale.edu/lab1/video_rec_service/proto"
	vmc "cs426.yale.edu/lab1/video_service/mock_client"
	vpb "cs426.yale.edu/lab1/video_service/proto"
	"github.com/influxdata/tdigest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	cacheRefreshBackoff = 10 * time.Second
	cacheRefreshTimeout = 30 * time.Second
)

type VideoRecServiceOptions struct {
	// Server address for the UserService"
	UserServiceAddr string
	// Server address for the VideoService
	VideoServiceAddr string
	// Maximum size of batches sent to UserService and VideoService
	MaxBatchSize int
	// If set, disable fallback to cache
	DisableFallback bool
	// If set, disable all retries
	DisableRetry bool
}

type VideoRecServiceStats struct {
	lock                   sync.Mutex
	td                     *tdigest.TDigest
	numReqs                uint64
	numReqsErr             uint64
	numReqsActive          uint64
	numReqsErrUserService  uint64
	numReqsErrVideoService uint64
	numReqsStale           uint64
	avgReqLatencyMs        float64
}

func MakeVideoRecServiceStats() *VideoRecServiceStats {
	return &VideoRecServiceStats{
		td: tdigest.New(),
	}
}

func (stats *VideoRecServiceStats) logRequestStart() time.Time {
	stats.lock.Lock()
	defer stats.lock.Unlock()

	stats.numReqs++
	stats.numReqsActive++
	return time.Now()
}

func (stats *VideoRecServiceStats) logRequestEnd(
	tStart time.Time,
	success bool,
) {
	reqLatencyMs := float64(time.Since(tStart)) / float64(time.Millisecond)

	stats.lock.Lock()
	defer stats.lock.Unlock()

	stats.numReqsActive--
	if !success {
		stats.numReqsErr++
	}
	stats.avgReqLatencyMs += (reqLatencyMs - stats.avgReqLatencyMs) /
		float64(stats.numReqs-stats.numReqsActive)
	stats.td.Add(reqLatencyMs, 1)
}

func (stats *VideoRecServiceStats) logUserServiceError() {
	stats.lock.Lock()
	defer stats.lock.Unlock()

	stats.numReqsErrUserService++
}

func (stats *VideoRecServiceStats) logVideoServiceError() {
	stats.lock.Lock()
	defer stats.lock.Unlock()

	stats.numReqsErrVideoService++
}

func (stats *VideoRecServiceStats) logStaleResponse() {
	stats.lock.Lock()
	defer stats.lock.Unlock()

	stats.numReqsStale++
}

type trendingCache struct {
	lock            sync.RWMutex
	videos          []*vpb.VideoInfo
	expirationTimeS uint64
	populated       bool
}

func (cache *trendingCache) store(
	videos []*vpb.VideoInfo,
	expirationTimeS uint64,
) {
	cache.lock.Lock()
	defer cache.lock.Unlock()

	cache.videos = videos
	cache.expirationTimeS = expirationTimeS
	cache.populated = true
}

func (cache *trendingCache) load() ([]*vpb.VideoInfo, bool) {
	cache.lock.RLock()
	defer cache.lock.RUnlock()

	return cache.videos, cache.populated
}

func (cache *trendingCache) expiration() time.Time {
	cache.lock.RLock()
	defer cache.lock.RUnlock()

	return time.Unix(int64(cache.expirationTimeS), 0)
}

type VideoRecServiceServer struct {
	pb.UnimplementedVideoRecServiceServer
	options            VideoRecServiceOptions
	stats              *VideoRecServiceStats
	userServiceClient  upb.UserServiceClient
	videoServiceClient vpb.VideoServiceClient
	cache              trendingCache
}

func MakeVideoRecServiceServer(options VideoRecServiceOptions) (
	*VideoRecServiceServer,
	error,
) {
	if options.MaxBatchSize <= 0 {
		return nil, status.Error(codes.InvalidArgument,
			"MaxBatchSize must be positive")
	}

	return &VideoRecServiceServer{
		options: options,
		stats:   MakeVideoRecServiceStats(),
	}, nil
}

func MakeVideoRecServiceServerWithMocks(
	options VideoRecServiceOptions,
	mockUserServiceClient *umc.MockUserServiceClient,
	mockVideoServiceClient *vmc.MockVideoServiceClient,
) *VideoRecServiceServer {
	return &VideoRecServiceServer{
		options:            options,
		stats:              MakeVideoRecServiceStats(),
		userServiceClient:  mockUserServiceClient,
		videoServiceClient: mockVideoServiceClient,
	}
}

func (server *VideoRecServiceServer) GetTopVideos(
	ctx context.Context,
	req *pb.GetTopVideosRequest,
) (resp *pb.GetTopVideosResponse, err error) {
	tStart := server.stats.logRequestStart()
	defer func() { server.stats.logRequestEnd(tStart, err == nil) }()

	resp, err = server.getPersonalizedVideos(ctx, req)
	if err == nil || server.options.DisableFallback {
		return resp, err
	}

	fallback, ok := server.getFallbackVideos(req.Limit)
	if !ok {
		return nil, err
	}
	log.Printf("serving fallback recommendations for user %d after error: %v",
		req.UserId, err)
	server.stats.logStaleResponse()
	return fallback, nil
}

func (server *VideoRecServiceServer) getPersonalizedVideos(
	ctx context.Context,
	req *pb.GetTopVideosRequest,
) (*pb.GetTopVideosResponse, error) {
	userServiceClient, userConn, err := server.getUserServiceClient()
	if err != nil {
		return nil, err
	}
	if userConn != nil {
		defer userConn.Close()
	}

	user, err := server.getUser(ctx, userServiceClient, req)
	if err != nil {
		return nil, err
	}

	likedVideos, err := server.getLikedVideos(ctx,
		userServiceClient, user.SubscribedTo, server.options.MaxBatchSize)
	if err != nil {
		return nil, err
	}

	videoServiceClient, videoConn, err := server.getVideoServiceClient()
	if err != nil {
		return nil, err
	}
	if videoConn != nil {
		defer videoConn.Close()
	}

	videoInfos, err := server.getVideoInfos(ctx,
		videoServiceClient, likedVideos, server.options.MaxBatchSize)
	if err != nil {
		return nil, err
	}

	return sortAndTruncate(scoreVideos(user, videoInfos), req.Limit), nil
}

func (server *VideoRecServiceServer) getFallbackVideos(
	limit int32,
) (*pb.GetTopVideosResponse, bool) {
	videos, populated := server.cache.load()
	if !populated {
		return nil, false
	}
	if limit > 0 && int(limit) < len(videos) {
		videos = videos[:limit]
	}
	return &pb.GetTopVideosResponse{
		Videos:        videos,
		StaleResponse: true,
	}, true
}

func withRetry[T any](
	disableRetry bool,
	logError func(),
	attempt func() (T, error),
) (T, error) {
	result, err := attempt()
	if err == nil {
		return result, nil
	}
	logError()
	if disableRetry {
		return result, err
	}
	result, err = attempt()
	if err != nil {
		logError()
	}
	return result, err
}

func (server *VideoRecServiceServer) getUserServiceClient() (
	upb.UserServiceClient, *grpc.ClientConn, error,
) {
	userServiceClient := server.userServiceClient
	if userServiceClient == nil {
		conn, err := withRetry(
			server.options.DisableRetry,
			server.stats.logUserServiceError,
			func() (*grpc.ClientConn, error) {
				return grpc.NewClient(server.options.UserServiceAddr,
					grpc.WithTransportCredentials(insecure.NewCredentials()))
			})
		if err != nil {
			log.Printf("failed to setup UserService client: %v", err)
			return nil, nil, status.Errorf(codes.Internal,
				"failed to setup UserService client: %v", err)
		}
		userServiceClient = upb.NewUserServiceClient(conn)
		return userServiceClient, conn, nil
	}
	return userServiceClient, nil, nil
}

func (server *VideoRecServiceServer) getVideoServiceClient() (
	vpb.VideoServiceClient, *grpc.ClientConn, error,
) {
	videoServiceClient := server.videoServiceClient
	if videoServiceClient == nil {
		conn, err := withRetry(
			server.options.DisableRetry,
			server.stats.logVideoServiceError,
			func() (*grpc.ClientConn, error) {
				return grpc.NewClient(server.options.VideoServiceAddr,
					grpc.WithTransportCredentials(insecure.NewCredentials()))
			})
		if err != nil {
			log.Printf("failed to setup VideoService client: %v", err)
			return nil, nil, status.Errorf(codes.Internal,
				"failed to setup VideoService client: %v", err)
		}
		videoServiceClient = vpb.NewVideoServiceClient(conn)
		return videoServiceClient, conn, nil
	}
	return videoServiceClient, nil, nil
}

func (server *VideoRecServiceServer) getUser(
	ctx context.Context,
	userServiceClient upb.UserServiceClient,
	req *pb.GetTopVideosRequest,
) (*upb.UserInfo, error) {
	userServiceResponse, err := withRetry(
		server.options.DisableRetry,
		server.stats.logUserServiceError,
		func() (*upb.GetUserResponse, error) {
			return userServiceClient.GetUser(ctx,
				&upb.GetUserRequest{UserIds: []uint64{req.UserId}})
		})
	if err != nil {
		log.Printf("GetUser failed for user %d: %v", req.UserId, err)
		return nil, status.Errorf(status.Code(err),
			"GetUser failed for user %d: %v", req.UserId, err)
	}
	if len(userServiceResponse.Users) != 1 {
		log.Printf("expected 1 user for id %d, got %d", req.UserId,
			len(userServiceResponse.Users))
		return nil, status.Errorf(codes.Internal,
			"expected 1 user for id %d, got %d", req.UserId,
			len(userServiceResponse.Users))
	}
	user := userServiceResponse.Users[0]
	return user, nil
}

func (server *VideoRecServiceServer) getLikedVideos(
	ctx context.Context,
	userServiceClient upb.UserServiceClient,
	userIds []uint64,
	maxBatchSize int,
) ([]uint64, error) {
	likedVideos := make([]uint64, 0)
	seenLikedVideos := make(map[uint64]bool)
	for start := 0; start < len(userIds); start += maxBatchSize {
		end := min(start+maxBatchSize, len(userIds))
		batch := userIds[start:end]
		userServiceResponse, err := withRetry(
			server.options.DisableRetry,
			server.stats.logUserServiceError,
			func() (*upb.GetUserResponse, error) {
				return userServiceClient.GetUser(ctx,
					&upb.GetUserRequest{UserIds: batch})
			})
		if err != nil {
			log.Printf("GetUser failed for users %v: %v", batch, err)
			return nil, status.Errorf(status.Code(err),
				"GetUser failed for users %v: %v", batch, err)
		}
		if len(userServiceResponse.Users) != len(batch) {
			log.Printf("expected %d users, got %d", len(batch),
				len(userServiceResponse.Users))
			return nil, status.Errorf(codes.Internal,
				"expected %d users, got %d", len(batch),
				len(userServiceResponse.Users))
		}
		for _, user := range userServiceResponse.Users {
			for _, likedVideo := range user.LikedVideos {
				if _, exists := seenLikedVideos[likedVideo]; !exists {
					seenLikedVideos[likedVideo] = true
					likedVideos = append(likedVideos, likedVideo)
				}
			}
		}
	}
	return likedVideos, nil
}

func (server *VideoRecServiceServer) getVideoInfos(
	ctx context.Context,
	videoServiceClient vpb.VideoServiceClient,
	videoIds []uint64,
	maxBatchSize int,
) ([]*vpb.VideoInfo, error) {
	videoInfos := make([]*vpb.VideoInfo, 0, len(videoIds))
	for start := 0; start < len(videoIds); start += maxBatchSize {
		end := min(start+maxBatchSize, len(videoIds))
		batch := videoIds[start:end]
		videoServiceResponse, err := withRetry(
			server.options.DisableRetry,
			server.stats.logVideoServiceError,
			func() (*vpb.GetVideoResponse, error) {
				return videoServiceClient.GetVideo(ctx,
					&vpb.GetVideoRequest{VideoIds: batch})
			})
		if err != nil {
			log.Printf("GetVideo failed for videos %v: %v", batch, err)
			return nil, status.Errorf(status.Code(err),
				"GetVideo failed for videos %v: %v", batch, err)
		}
		if len(videoServiceResponse.Videos) != len(batch) {
			log.Printf("expected %d videos, got %d", len(batch),
				len(videoServiceResponse.Videos))
			return nil, status.Errorf(codes.Internal,
				"expected %d videos, got %d", len(batch),
				len(videoServiceResponse.Videos))
		}
		videoInfos = append(videoInfos, videoServiceResponse.Videos...)
	}
	return videoInfos, nil
}

type ScoredVideo struct {
	videoInfo *vpb.VideoInfo
	score     uint64
}

func scoreVideos(
	user *upb.UserInfo,
	videoInfos []*vpb.VideoInfo,
) []ScoredVideo {
	scoredVideos := make([]ScoredVideo, 0, len(videoInfos))
	for _, videoInfo := range videoInfos {
		score := (&ranker.BcryptRanker{}).Rank(
			user.UserCoefficients,
			videoInfo.VideoCoefficients,
		)
		scoredVideos = append(scoredVideos, ScoredVideo{videoInfo, score})
	}
	return scoredVideos
}

func sortAndTruncate(
	scoredVideos []ScoredVideo,
	limit int32,
) *pb.GetTopVideosResponse {
	sort.SliceStable(scoredVideos, func(i, j int) bool {
		return scoredVideos[i].score > scoredVideos[j].score
	})
	var videoInfos []*vpb.VideoInfo
	if limit <= 0 {
		videoInfos = make([]*vpb.VideoInfo, len(scoredVideos))
	} else {
		videoInfos = make([]*vpb.VideoInfo,
			min(int(limit), len(scoredVideos)))
	}
	for i := range len(videoInfos) {
		videoInfos[i] = scoredVideos[i].videoInfo
	}
	return &pb.GetTopVideosResponse{Videos: videoInfos}
}

func (server *VideoRecServiceServer) GetStats(
	ctx context.Context,
	req *pb.GetStatsRequest,
) (*pb.GetStatsResponse, error) {
	server.stats.lock.Lock()
	defer server.stats.lock.Unlock()

	var p99ReqLatencyMs float32
	if server.stats.td.Count() == 0 {
		p99ReqLatencyMs = 0
	} else {
		p99ReqLatencyMs = float32(server.stats.td.Quantile(0.99))
	}

	return &pb.GetStatsResponse{
		TotalRequests:      server.stats.numReqs,
		TotalErrors:        server.stats.numReqsErr,
		ActiveRequests:     server.stats.numReqsActive,
		UserServiceErrors:  server.stats.numReqsErrUserService,
		VideoServiceErrors: server.stats.numReqsErrVideoService,
		AverageLatencyMs:   float32(server.stats.avgReqLatencyMs),
		P99LatencyMs:       p99ReqLatencyMs,
		StaleResponses:     server.stats.numReqsStale,
	}, nil
}

func (server *VideoRecServiceServer) RefreshCache(ctx context.Context) error {
	videoServiceClient, videoConn, err := server.getVideoServiceClient()
	if err != nil {
		return err
	}
	if videoConn != nil {
		defer videoConn.Close()
	}

	trendingResponse, err := withRetry(
		server.options.DisableRetry,
		server.stats.logVideoServiceError,
		func() (*vpb.GetTrendingVideosResponse, error) {
			return videoServiceClient.GetTrendingVideos(ctx,
				&vpb.GetTrendingVideosRequest{})
		})
	if err != nil {
		log.Printf("GetTrendingVideos failed: %v", err)
		return status.Errorf(status.Code(err),
			"GetTrendingVideos failed: %v", err)
	}

	videoInfos, err := server.getVideoInfos(ctx, videoServiceClient,
		trendingResponse.Videos, server.options.MaxBatchSize)
	if err != nil {
		return err
	}

	server.cache.store(videoInfos, trendingResponse.ExpirationTimeS)
	return nil
}

func (server *VideoRecServiceServer) ContinuallyRefreshCache() {
	for {
		ctx, cancel := context.WithTimeout(context.Background(),
			cacheRefreshTimeout)
		err := server.RefreshCache(ctx)
		cancel()

		if err != nil {
			log.Printf("trending cache refresh failed, retrying in %v: %v",
				cacheRefreshBackoff, err)
			time.Sleep(cacheRefreshBackoff)
			continue
		}

		wait := max(time.Until(server.cache.expiration()),
			cacheRefreshBackoff)
		log.Printf("trending cache refreshed, next refresh in %v", wait)
		time.Sleep(wait)
	}
}
