package cache_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nix-community/go-nix/pkg/narinfo"
	"github.com/nix-community/go-nix/pkg/narinfo/signature"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kalbasit/ncps/pkg/cache"
	"github.com/kalbasit/ncps/pkg/cache/upstream"
	"github.com/kalbasit/ncps/pkg/database"
	"github.com/kalbasit/ncps/pkg/nar"
	"github.com/kalbasit/ncps/pkg/storage"
	"github.com/kalbasit/ncps/pkg/storage/local"
	"github.com/kalbasit/ncps/testdata"
	"github.com/kalbasit/ncps/testhelper"

	// Import the SQLite driver.
	_ "github.com/mattn/go-sqlite3"
)

const cacheName = "cache.example.com"

func newContext() context.Context {
	return zerolog.
		New(io.Discard).
		WithContext(context.Background())
}

// setupTestCache is a helper to create an isolated test environment for each test.
func setupTestCache(t *testing.T) (*cache.Cache, *testdata.Server, *local.Store) {
	t.Helper()

	ctx := newContext()
	upstreamServer := testdata.NewTestServer(t, 1)
	t.Cleanup(upstreamServer.Close)

	upstreamCache, err := upstream.New(ctx, testhelper.MustParseURL(t, upstreamServer.URL), testdata.PublicKeys())
	require.NoError(t, err)

	dbDir, err := os.MkdirTemp("", "cache-db-")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dbDir) })

	dbFile := filepath.Join(dbDir, "db.sqlite")
	testhelper.CreateMigrateDatabase(t, dbFile)
	db, err := database.Open("sqlite:" + dbFile)
	require.NoError(t, err)

	storeDir, err := os.MkdirTemp("", "ncps-store-")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(storeDir) })
	simpleLocalStore, err := local.New(ctx, storeDir)
	require.NoError(t, err)

	cacheInstance, err := cache.New(ctx, cacheName, db, simpleLocalStore, simpleLocalStore, simpleLocalStore, "")
	require.NoError(t, err)
	cacheInstance.AddUpstreamCaches(ctx, upstreamCache)
	cacheInstance.SetRecordAgeIgnoreTouch(0)

	return cacheInstance, upstreamServer, simpleLocalStore
}

//nolint:paralleltest
func TestGetNarInfo(t *testing.T) {
	c, _, localStore := setupTestCache(t)
	ctx := newContext()

	t.Run("narinfo does not exist upstream", func(t *testing.T) {
		_, err := c.GetNarInfo(ctx, "doesnotexist")
		assert.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("narinfo exists upstream", func(t *testing.T) {
		ni, err := c.GetNarInfo(ctx, testdata.Nar2.NarInfoHash)
		require.NoError(t, err)

		storePath := filepath.Join(localStore.Path(), "store", "narinfo", testdata.Nar2.NarInfoPath)

		t.Run("size is correct", func(t *testing.T) {
			assert.Equal(t, uint64(50308), ni.FileSize)
		})

		t.Run("it should now exist in the store", func(t *testing.T) {
			assert.FileExists(t, storePath)
		})

		t.Run("it should have also pulled the nar", func(t *testing.T) {
			narPath := filepath.Join(localStore.Path(), "store", "nar", testdata.Nar2.NarPath)
			assert.FileExists(t, narPath)
		})
	})
}

//nolint:funlen,cyclop
func TestGetNar_Concurrent(t *testing.T) {
	t.Parallel()

	cacheInstance, upstreamServer, localStore := setupTestCache(t)
	ctx := newContext()

	narHash := "concurrentNarHash"
	narContent := "concurrent NAR content"
	narURL := nar.URL{Hash: narHash, Compression: nar.CompressionTypeNone}

	// --- Test Setup ---
	var upstreamHitCount int32
	// Use a channel to control when the upstream responds, to test the blocking behavior.
	releaseUpstream := make(chan struct{})

	upstreamServer.AddMaybeHandler(func(w http.ResponseWriter, r *http.Request) bool {
		if strings.Contains(r.URL.Path, narHash) {
			atomic.AddInt32(&upstreamHitCount, 1)
			<-releaseUpstream // Block until the test signals to continue.
			w.Header().Set("Content-Length", fmt.Sprint(len(narContent)))
			_, err := io.WriteString(w, narContent)
			require.NoError(t, err)
			return true
		}
		return false
	})

	var wg sync.WaitGroup
	errs := make(chan error, 2)

	// --- Leader Goroutine ---
	// This goroutine will be the first to request the NAR. It should receive a streaming response.
	wg.Add(1)
	go func() {
		defer wg.Done()
		t.Log("Leader: Requesting NAR...")
		size, reader, err := cacheInstance.GetNar(ctx, narURL)
		if err != nil {
			errs <- fmt.Errorf("leader GetNar returned error: %w", err)
			return
		}
		defer reader.Close()

		assert.Equal(t, int64(-1), size, "Leader should get unknown size for streaming response")

		readBytes, err := io.ReadAll(reader)
		if err != nil {
			errs <- fmt.Errorf("leader ReadAll error: %w", err)
			return
		}
		assert.Equal(t, narContent, string(readBytes))
		t.Log("Leader: Finished reading stream.")
	}()

	// --- Follower Goroutine ---
	// This goroutine will request the same NAR while the leader's download is "in-flight".
	// It should block until the download is complete and then read from the cache.
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Give the leader a moment to establish the download job
		time.Sleep(20 * time.Millisecond)
		t.Log("Follower: Requesting NAR...")
		size, reader, err := cacheInstance.GetNar(ctx, narURL)
		if err != nil {
			errs <- fmt.Errorf("follower GetNar returned error: %w", err)
			return
		}
		defer reader.Close()

		// The size should be known because it read from the fully cached file.
		assert.Equal(t, int64(len(narContent)), size, "Follower should get correct size from cached file")

		readBytes, err := io.ReadAll(reader)
		if err != nil {
			errs <- fmt.Errorf("follower ReadAll error: %w", err)
			return
		}
		assert.Equal(t, narContent, string(readBytes))
		t.Log("Follower: Finished reading from cache.")
	}()

	// Give both goroutines a chance to start up and get to the point where they are waiting.
	time.Sleep(100 * time.Millisecond)
	// Now, unblock the upstream download.
	t.Log("Main: Releasing upstream...")
	close(releaseUpstream)

	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	// Final assertions
	assert.Equal(t, int32(1), atomic.LoadInt32(&upstreamHitCount), "Upstream should only be hit once")
	narPath := filepath.Join(localStore.Path(), "store", "nar", narURL.ToFilePath())
	assert.FileExists(t, narPath, "NAR file should be cached on disk")
}

//nolint:paralleltest
func TestPutNar(t *testing.T) {
	cacheInstance, _, localStore := setupTestCache(t)
	ctx := newContext()

	storePath := filepath.Join(localStore.Path(), "store", "nar", testdata.Nar1.NarPath)

	t.Run("nar does not exist in storage yet", func(t *testing.T) {
		assert.NoFileExists(t, storePath)
	})

	t.Run("putNar does not return an error", func(t *testing.T) {
		r := io.NopCloser(strings.NewReader(testdata.Nar1.NarText))
		nu := nar.URL{Hash: testdata.Nar1.NarHash, Compression: testdata.Nar1.NarCompression}
		written, err := cacheInstance.PutNar(ctx, nu, r)
		assert.NoError(t, err)
		assert.EqualValues(t, len(testdata.Nar1.NarText), written)
	})

	t.Run("nar does exist in storage", func(t *testing.T) {
		f, err := os.Open(storePath)
		require.NoError(t, err)
		defer f.Close()

		bs, err := io.ReadAll(f)
		require.NoError(t, err)
		assert.Equal(t, testdata.Nar1.NarText, string(bs))
	})
}

//nolint:paralleltest
func TestPutNarInfo(t *testing.T) {
	cacheInstance, _, localStore := setupTestCache(t)
	ctx := newContext()

	storePath := filepath.Join(localStore.Path(), "store", "narinfo", testdata.Nar1.NarInfoPath)

	t.Run("narinfo does not exist in storage yet", func(t *testing.T) {
		assert.NoFileExists(t, storePath)
	})

	t.Run("PutNarInfo does not return an error", func(t *testing.T) {
		r := io.NopCloser(strings.NewReader(testdata.Nar1.NarInfoText))
		assert.NoError(t, cacheInstance.PutNarInfo(ctx, testdata.Nar1.NarInfoHash, r))
	})

	t.Run("it should be signed by our server", func(t *testing.T) {
		f, err := os.Open(storePath)
		require.NoError(t, err)
		defer f.Close()

		ni, err := narinfo.Parse(f)
		require.NoError(t, err)

		var found bool
		for _, sig := range ni.Signatures {
			if sig.Name == cacheName {
				found = true
				break
			}
		}
		assert.True(t, found, "narinfo should be signed by our cache")
		assert.True(t, signature.VerifyFirst(ni.Fingerprint(), ni.Signatures, []signature.PublicKey{cacheInstance.PublicKey()}))
	})
}
