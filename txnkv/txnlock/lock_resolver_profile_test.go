package txnlock

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	logpkg "github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/internal/apicodec"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/mockstore/mocktikv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/oracle/oracles"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
)

const lockResolverProfileRootDir = "/Users/wangchao/Downloads/lock-resolver-profile-tmp"
const testDuration = 10 * time.Minute
const lockCount = 20000

type profileTestStorage struct {
	regionCache *locate.RegionCache
	client      client.Client
	oracle      oracle.Oracle
}

func (s *profileTestStorage) GetRegionCache() *locate.RegionCache {
	return s.regionCache
}

func (s *profileTestStorage) SendReq(bo *retry.Backoffer, req *tikvrpc.Request, regionID locate.RegionVerID, timeout time.Duration) (*tikvrpc.Response, error) {
	sender := locate.NewRegionRequestSender(s.regionCache, s.client, oracle.NoopReadTSValidator{})
	resp, _, err := sender.SendReq(bo, req, regionID, timeout)
	return resp, err
}

func (s *profileTestStorage) GetOracle() oracle.Oracle {
	return s.oracle
}

func TestResolveLocksForReadAsyncProfile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping async resolve profiling test in short mode")
	}

	logger, props, err := logpkg.InitLogger(&logpkg.Config{
		Level:               "error",
		DisableTimestamp:    true,
		DisableStacktrace:   true,
		DisableErrorVerbose: true,
		File:                logpkg.FileLogConfig{},
	})
	require.NoError(t, err)
	restoreLogger := logpkg.ReplaceGlobals(logger, props)
	defer restoreLogger()

	mvccStore := mocktikv.MustNewMVCCStore()
	defer mvccStore.Close()

	cluster := mocktikv.NewCluster(mvccStore)
	_, _, _, _ = mocktikv.BootstrapWithMultiStores(cluster, 3)

	pdClient := locate.NewCodecPDClient(apicodec.ModeTxn, mocktikv.NewPDClient(cluster))
	regionCache := locate.NewRegionCache(pdClient)
	defer regionCache.Close()

	rpcClient := mocktikv.NewRPCClient(cluster, mvccStore, nil)
	oracleClient := &oracles.MockOracle{}
	store := &profileTestStorage{
		regionCache: regionCache,
		client:      rpcClient,
		oracle:      oracleClient,
	}
	lockResolver := NewLockResolver(store)
	defer lockResolver.Close()

	const txnID = uint64(100)
	const commitTS = uint64(101)
	const callerStartTS = uint64(200)
	lockResolver.saveResolved(txnID, TxnStatus{commitTS: commitTS})

	locks := make([]*Lock, 0, lockCount)
	for i := 0; i < lockCount; i++ {
		key := []byte(fmt.Sprintf("k-%05d", i))
		locks = append(locks, &Lock{
			Key:         key,
			Primary:     []byte("primary"),
			TxnID:       txnID,
			TxnSize:     1,
			LockType:    0,
			TTL:         0,
			MinCommitTS: 0,
		})
	}

	require.NoError(t, os.MkdirAll(lockResolverProfileRootDir, 0o755))
	profileDir, err := os.MkdirTemp(
		lockResolverProfileRootDir,
		fmt.Sprintf("resolve-locks-fix-%d-%dm", lockCount, testDuration/time.Minute)+"-"+
			time.Now().Format("20060102150405")+"-",
	)
	require.NoError(t, err)
	cpuProfilePath := profileDir + "/cpu.pprof"
	heapProfilePath := profileDir + "/heap.pprof"
	goroutineProfilePath := profileDir + "/goroutine.pprof"
	testLogPath := profileDir + "/test.log"

	cpuFile, err := os.Create(cpuProfilePath)
	require.NoError(t, err)
	require.NoError(t, pprof.StartCPUProfile(cpuFile))
	cpuProfileStopped := false
	stopCPUProfile := func() {
		if cpuProfileStopped {
			return
		}
		pprof.StopCPUProfile()
		cpuProfileStopped = true
		require.NoError(t, cpuFile.Close())
	}
	defer stopCPUProfile()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = util.WithInternalSourceType(ctx, "MockResolveLockBusy")
	bo := retry.NewBackoffer(ctx, 3600000)
	go func() {
		_, _, _, _ = lockResolver.ResolveLocksForRead(bo, callerStartTS, locks, true)
	}()

	start := time.Now()
	ratio := int64(0)
	for ratio < 100 {
		time.Sleep(time.Second)
		curRatio := min(int64(time.Now().Sub(start)*100/testDuration), 100)
		if curRatio > ratio {
			t.Logf("waiting for resolve locks to finish... %d%%", curRatio)
			ratio = curRatio
		}
	}

	t.Logf("collecting profiles...")
	stopCPUProfile()
	goroutines := runtime.NumGoroutine()
	runtime.GC()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	writeNamedProfile(t, "heap", heapProfilePath, 0)
	writeNamedProfile(t, "goroutine", goroutineProfilePath, 1)

	assertProfileFile(t, cpuProfilePath)
	assertProfileFile(t, heapProfilePath)
	assertProfileFile(t, goroutineProfilePath)

	cancel()
	time.Sleep(2 * time.Second)
	logLines := []string{
		fmt.Sprintf("async resolve profile dir:\n%s\n", profileDir),
		fmt.Sprintf(
			"goroutines %d, heap_alloc=%.2fMB, heap_inuse=%.2fMB",
			goroutines,
			float64(mem.HeapAlloc)/1000000,
			float64(mem.HeapInuse)/1000000,
		),
	}
	for _, line := range logLines {
		t.Log(line)
	}
	require.NoError(t, writeTestLog(testLogPath, logLines))
}

func writeNamedProfile(t *testing.T, profileName, path string, debug int) {
	t.Helper()

	if profileName == "heap" {
		runtime.GC()
	}

	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()

	profile := pprof.Lookup(profileName)
	require.NotNil(t, profile)
	require.NoError(t, profile.WriteTo(f, debug))
}

func assertProfileFile(t *testing.T, path string) {
	t.Helper()

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.NotZero(t, info.Size(), "profile %s should not be empty", path)
}

func writeTestLog(path string, lines []string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, line := range lines {
		if _, err := fmt.Fprintln(f, line); err != nil {
			return err
		}
	}
	return nil
}
