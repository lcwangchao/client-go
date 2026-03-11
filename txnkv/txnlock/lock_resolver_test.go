package txnlock

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/mockstore/mocktikv"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/oracle/oracles"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
)

type testLockResolverStorage struct {
	regionCache *locate.RegionCache
	client      *mocktikv.RPCClient
	oracle      oracle.Oracle
}

func (s *testLockResolverStorage) GetRegionCache() *locate.RegionCache {
	return s.regionCache
}

func (s *testLockResolverStorage) SendReq(bo *retry.Backoffer, req *tikvrpc.Request, regionID locate.RegionVerID, timeout time.Duration) (*tikvrpc.Response, error) {
	rpcCtx, err := s.regionCache.GetTiKVRPCContext(bo, regionID, kv.ReplicaReadLeader, 0)
	if err != nil {
		return nil, err
	}
	if err := tikvrpc.SetContextNoAttach(req, rpcCtx.Meta, rpcCtx.Peer); err != nil {
		return nil, err
	}
	return s.client.SendRequest(bo.GetCtx(), rpcCtx.Addr, req, timeout)
}

func (s *testLockResolverStorage) GetOracle() oracle.Oracle {
	return s.oracle
}

// TestLockResolverCache is used to cover the issue https://github.com/pingcap/tidb/issues/59494.
func TestLockResolverCache(t *testing.T) {
	util.EnableFailpoints()
	lockResolver := NewLockResolver(nil)
	lock := func(key, primary string, startTS uint64, useAsyncCommit bool, secondaries [][]byte) *kvrpcpb.LockInfo {
		return &kvrpcpb.LockInfo{
			Key:            []byte(key),
			PrimaryLock:    []byte(primary),
			LockVersion:    startTS,
			UseAsyncCommit: useAsyncCommit,
			MinCommitTs:    startTS + 1,
			Secondaries:    secondaries,
		}
	}

	resolvedTxnTS := uint64(1)
	k1 := "k1"
	k2 := "k2"
	resolvedTxnStatus := TxnStatus{
		ttl:         0,
		commitTS:    10,
		primaryLock: lock(k1, k1, resolvedTxnTS, true, [][]byte{[]byte(k2)}),
	}
	lockResolver.mu.resolved[resolvedTxnTS] = resolvedTxnStatus
	toResolveLock := lock(k2, k1, resolvedTxnTS, true, [][]byte{})
	backOff := retry.NewBackoffer(context.Background(), asyncResolveLockMaxBackoff)

	// Save the async commit transaction resolved result to the resolver cache.
	lockResolver.saveResolved(resolvedTxnTS, resolvedTxnStatus)

	// With failpoint, the async commit transaction will be resolved and `CheckSecondaries` would not be called.
	// Otherwise, the test would panic as the storage is nil.
	require.Nil(t, failpoint.Enable("tikvclient/resolveAsyncCommitLockReturn", "return"))
	_, err := lockResolver.ResolveLocks(backOff, 5, []*Lock{NewLock(toResolveLock)})
	require.NoError(t, err)
	require.Nil(t, failpoint.Disable("tikvclient/resolveAsyncCommitLockReturn"))
}

func TestLockResolverResolvedDebugTracksPrimaryAndCacheHit(t *testing.T) {
	lockResolver := NewLockResolver(nil)
	txnID := uint64(42)
	primary := []byte("primary-key")
	status := TxnStatus{
		commitTS: 100,
	}

	lockResolver.saveResolvedWithTrace(txnID, status, primary, "initial-trace")

	debugInfo := lockResolver.mu.resolvedDebug[txnID]
	require.Equal(t, primary, debugInfo.primaryKey)
	require.Len(t, debugInfo.history, 1)
	require.Contains(t, debugInfo.history[0], "kind=save")
	require.Contains(t, debugInfo.history[0], "primary=7072696D6172792D6B6579")

	bo := retry.NewBackoffer(context.Background(), 1)
	got, err := lockResolver.getTxnStatus(bo, txnID, primary, 11, 22, false, false, &Lock{TxnID: txnID, Primary: primary})
	require.NoError(t, err)
	require.Equal(t, status, got)

	debugInfo = lockResolver.mu.resolvedDebug[txnID]
	require.Equal(t, primary, debugInfo.primaryKey)
	require.Len(t, debugInfo.history, 2)
	require.Contains(t, debugInfo.history[1], "kind=cache-hit")
	require.Contains(t, debugInfo.history[1], "primary=7072696D6172792D6B6579")
}

func TestLockResolverGetMvccByKeyDebugInfoWithMockStore(t *testing.T) {
	mvccStore := mocktikv.MustNewMVCCStore()
	cluster := mocktikv.NewCluster(mvccStore)
	mocktikv.BootstrapWithSingleStore(cluster)
	client := mocktikv.NewRPCClient(cluster, mvccStore, nil)
	defer func() {
		require.NoError(t, client.Close())
	}()
	pdClient := mocktikv.NewPDClient(cluster)
	store := &testLockResolverStorage{
		regionCache: locate.NewRegionCache(pdClient),
		client:      client,
		oracle:      &oracles.MockOracle{},
	}

	ok := mocktikv.MustPrewrite(mvccStore, mocktikv.PutMutations("pk", "value"), "pk", 5, 3000)
	require.True(t, ok)
	require.NoError(t, mvccStore.Commit([][]byte{[]byte("pk")}, 5, 10))

	lockResolver := NewLockResolver(store)
	mvccInfo := lockResolver.getMvccByKeyDebugInfo([]byte("pk"))
	require.Contains(t, mvccInfo, "key=706B")
	require.Contains(t, mvccInfo, "writes=[{type:Put start_ts:5 commit_ts:10")
	require.Contains(t, mvccInfo, "values=[{start_ts:5 value:76616C7565}]")
}

func TestLockResolverMismatchSavePreservesHistory(t *testing.T) {
	lockResolver := NewLockResolver(nil)
	txnID := uint64(7)
	primary := []byte("pk")
	oldStatus := TxnStatus{commitTS: 11}
	newStatus := TxnStatus{commitTS: 12}

	lockResolver.saveResolvedWithTrace(txnID, oldStatus, primary, "old-trace")

	require.PanicsWithValue(t, "unexpected txn status saved to cache with existing different entry", func() {
		lockResolver.saveResolvedWithTrace(txnID, newStatus, primary, "new-trace")
	})

	debugInfo := lockResolver.mu.resolvedDebug[txnID]
	require.Equal(t, primary, debugInfo.primaryKey)
	require.Len(t, debugInfo.history, 2)
	require.Contains(t, debugInfo.history[1], "kind=mismatch-save")
	require.True(t, strings.Contains(debugInfo.history[1], "status=\"ttl:0 commit_ts:12 action: NoAction\""))
}
