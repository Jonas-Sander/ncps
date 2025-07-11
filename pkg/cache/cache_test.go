package cache_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
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

// mockNarStore is a mock implementation of storage.NarStore for testing.
type mockNarStore struct {
	storage.NarStore // Embed the interface
	hasNarFunc       func(ctx context.Context, narURL nar.URL) bool
	getNarFunc       func(ctx context.Context, narURL nar.URL) (int64, io.ReadCloser, error)
	putNarFunc       func(ctx context.Context, narURL nar.URL, r io.Reader) (int64, error)
	deleteNarFunc    func(ctx context.Context, narURL nar.URL) error
	// For asserting calls
	putNarCalls []nar.URL
	mu          sync.Mutex
}

func (m *mockNarStore) HasNar(ctx context.Context, narURL nar.URL) bool {
	if m.hasNarFunc != nil {
		return m.hasNarFunc(ctx, narURL)
	}
	return false
}

func (m *mockNarStore) GetNar(ctx context.Context, narURL nar.URL) (int64, io.ReadCloser, error) {
	if m.getNarFunc != nil {
		return m.getNarFunc(ctx, narURL)
	}
	return 0, nil, storage.ErrNotFound
}

func (m *mockNarStore) PutNar(ctx context.Context, narURL nar.URL, r io.Reader) (int64, error) {
	m.mu.Lock()
	m.putNarCalls = append(m.putNarCalls, narURL)
	m.mu.Unlock()
	// It's important to consume the reader r to simulate real storage.
	written, copyErr := io.Copy(io.Discard, r)
	if m.putNarFunc != nil {
		// Call the custom func, but pass along any copy error.
		// The custom func can decide what to do with 'written'.
		_, err := m.putNarFunc(ctx, narURL, r) // r is already consumed by io.Copy
		if err != nil {
			return written, err // Return custom error
		}
	}
	return written, copyErr // Return result of io.Copy if no custom error
}

func (m *mockNarStore) DeleteNar(ctx context.Context, narURL nar.URL) error {
	if m.deleteNarFunc != nil {
		return m.deleteNarFunc(ctx, narURL)
	}
	return nil
}

func (m *mockNarStore) GetPutNarCalls() []nar.URL {
	m.mu.Lock()
	defer m.mu.Unlock()
	calls := make([]nar.URL, len(m.putNarCalls))
	copy(calls, m.putNarCalls)
	return calls
}

func (m *mockNarStore) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.putNarCalls = nil
	m.hasNarFunc = nil
	m.getNarFunc = nil
	m.putNarFunc = nil
	m.deleteNarFunc = nil
}

func newContext() context.Context {
	return zerolog.
		New(io.Discard).
		WithContext(context.Background())
}

// setupTestCacheWithMocks is a helper to create an isolated test environment.
func setupTestCacheWithMocks(t *testing.T, ctx context.Context, upstreamServer *testdata.Server) (*cache.Cache, *mockNarStore) {
	t.Helper()

	upstreamCache, err := upstream.New(ctx, testhelper.MustParseURL(t, upstreamServer.URL), nil)
	require.NoError(t, err)

	dbDir, err := os.MkdirTemp("", "cache-db-")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dbDir) })

	dbFile := filepath.Join(dbDir, "db.sqlite")
	testhelper.CreateMigrateDatabase(t, dbFile)
	db, err := database.Open("sqlite:" + dbFile)
	require.NoError(t, err)

	mockNarStoreInstance := &mockNarStore{}

	storeDir, err := os.MkdirTemp("", "ncps-store-")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(storeDir) })
	simpleLocalStore, err := local.New(ctx, storeDir)
	require.NoError(t, err)

	cacheInstance, err := cache.New(ctx, cacheName, db, simpleLocalStore, simpleLocalStore, mockNarStoreInstance, "")
	require.NoError(t, err)
	cacheInstance.AddUpstreamCaches(ctx, upstreamCache)
	cacheInstance.SetRecordAgeIgnoreTouch(0)

	return cacheInstance, mockNarStoreInstance
}

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("hostname must be valid with no scheme or path", func(t *testing.T) {
		t.Parallel()

		t.Run("hostname must not be empty", func(t *testing.T) {
			t.Parallel()

			dir, err := os.MkdirTemp("", "cache-path-")
			require.NoError(t, err)
			defer os.RemoveAll(dir) // clean up

			dbFile := filepath.Join(dir, "var", "ncps", "db", "db.sqlite")
			testhelper.CreateMigrateDatabase(t, dbFile)

			db, err := database.Open("sqlite:" + dbFile)
			require.NoError(t, err)

			localStore, err := local.New(newContext(), dir)
			require.NoError(t, err)

			_, err = cache.New(newContext(), "", db, localStore, localStore, localStore, "")
			assert.ErrorIs(t, err, cache.ErrHostnameRequired)
		})

		t.Run("hostname must not contain scheme", func(t *testing.T) {
			t.Parallel()

			dir, err := os.MkdirTemp("", "cache-path-")
			require.NoError(t, err)
			defer os.RemoveAll(dir) // clean up

			dbFile := filepath.Join(dir, "var", "ncps", "db", "db.sqlite")
			testhelper.CreateMigrateDatabase(t, dbFile)

			db, err := database.Open("sqlite:" + dbFile)
			require.NoError(t, err)

			localStore, err := local.New(newContext(), dir)
			require.NoError(t, err)

			_, err = cache.New(newContext(), "https://cache.example.com", db, localStore, localStore, localStore, "")
			assert.ErrorIs(t, err, cache.ErrHostnameMustNotContainScheme)
		})

		t.Run("hostname must not contain a path", func(t *testing.T) {
			t.Parallel()

			dir, err := os.MkdirTemp("", "cache-path-")
			require.NoError(t, err)
			defer os.RemoveAll(dir) // clean up

			dbFile := filepath.Join(dir, "var", "ncps", "db", "db.sqlite")
			testhelper.CreateMigrateDatabase(t, dbFile)

			db, err := database.Open("sqlite:" + dbFile)
			require.NoError(t, err)

			localStore, err := local.New(newContext(), dir)
			require.NoError(t, err)

			_, err = cache.New(newContext(), "cache.example.com/path/to", db, localStore, localStore, localStore, "")
			assert.ErrorIs(t, err, cache.ErrHostnameMustNotContainPath)
		})

		t.Run("valid hostName must return no error", func(t *testing.T) {
			t.Parallel()

			dir, err := os.MkdirTemp("", "cache-path-")
			require.NoError(t, err)
			defer os.RemoveAll(dir) // clean up

			dbFile := filepath.Join(dir, "var", "ncps", "db", "db.sqlite")
			testhelper.CreateMigrateDatabase(t, dbFile)

			db, err := database.Open("sqlite:" + dbFile)
			require.NoError(t, err)

			localStore, err := local.New(newContext(), dir)
			require.NoError(t, err)

			_, err = cache.New(newContext(), cacheName, db, localStore, localStore, localStore, "")
			require.NoError(t, err)
		})
	})

	t.Run("secretKey", func(t *testing.T) {
		t.Parallel()

		t.Run("generated", func(t *testing.T) {
			t.Parallel()

			dir, err := os.MkdirTemp("", "cache-path-")
			require.NoError(t, err)
			defer os.RemoveAll(dir) // clean up

			dbFile := filepath.Join(dir, "var", "ncps", "db", "db.sqlite")
			testhelper.CreateMigrateDatabase(t, dbFile)

			db, err := database.Open("sqlite:" + dbFile)
			require.NoError(t, err)

			localStore, err := local.New(newContext(), dir)
			require.NoError(t, err)

			c, err := cache.New(newContext(), cacheName, db, localStore, localStore, localStore, "")
			require.NoError(t, err)

			sk, err := localStore.GetSecretKey(newContext())
			require.NoError(t, err)

			assert.Equal(t, sk.ToPublicKey(), c.PublicKey(), "ensure the cache public key matches the one in the local store")
		})

		t.Run("given", func(t *testing.T) {
			t.Parallel()

			dir, err := os.MkdirTemp("", "cache-path-")
			require.NoError(t, err)
			defer os.RemoveAll(dir) // clean up

			dbFile := filepath.Join(dir, "var", "ncps", "db", "db.sqlite")
			testhelper.CreateMigrateDatabase(t, dbFile)

			db, err := database.Open("sqlite:" + dbFile)
			require.NoError(t, err)

			localStore, err := local.New(newContext(), dir)
			require.NoError(t, err)

			sk, _, err := signature.GenerateKeypair(cacheName, nil)
			require.NoError(t, err)

			skFile, err := os.CreateTemp("", "secret-key")
			require.NoError(t, err)

			defer os.Remove(skFile.Name())

			_, err = skFile.WriteString(sk.String())
			require.NoError(t, err)

			require.NoError(t, skFile.Close())

			c, err := cache.New(newContext(), cacheName, db, localStore, localStore, localStore, skFile.Name())
			require.NoError(t, err)

			_, err = localStore.GetSecretKey(newContext())
			require.ErrorIs(t, err, storage.ErrNotFound)

			assert.Equal(t, sk.ToPublicKey(), c.PublicKey(), "ensure the cache public key matches the one given")
		})
	})
}

func TestPublicKey(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "cache-path-")
	require.NoError(t, err)
	defer os.RemoveAll(dir) // clean up

	dbFile := filepath.Join(dir, "var", "ncps", "db", "db.sqlite")
	testhelper.CreateMigrateDatabase(t, dbFile)

	db, err := database.Open("sqlite:" + dbFile)
	require.NoError(t, err)

	localStore, err := local.New(newContext(), dir)
	require.NoError(t, err)

	c, err := cache.New(newContext(), cacheName, db, localStore, localStore, localStore, "")
	require.NoError(t, err)

	pubKey := c.PublicKey().String()

	t.Run("should return a public key with the correct prefix", func(t *testing.T) {
		t.Parallel()

		assert.True(t, strings.HasPrefix(pubKey, "cache.example.com:"))
	})

	t.Run("should return a valid public key", func(t *testing.T) {
		t.Parallel()

		pk, err := signature.ParsePublicKey(pubKey)
		require.NoError(t, err)

		assert.Equal(t, pubKey, pk.String())
	})
}

func TestGetNarInfoWithoutSignature(t *testing.T) {
	t.Parallel()

	ts := testdata.NewTestServer(t, 40)
	defer ts.Close()

	dir, err := os.MkdirTemp("", "cache-path-")
	require.NoError(t, err)
	defer os.RemoveAll(dir) // clean up

	uc, err := upstream.New(newContext(), testhelper.MustParseURL(t, ts.URL), testdata.PublicKeys())
	require.NoError(t, err)

	dbFile := filepath.Join(dir, "var", "ncps", "db", "db.sqlite")
	testhelper.CreateMigrateDatabase(t, dbFile)

	db, err := database.Open("sqlite:" + dbFile)
	require.NoError(t, err)

	localStore, err := local.New(newContext(), dir)
	require.NoError(t, err)

	c, err := cache.New(newContext(), cacheName, db, localStore, localStore, localStore, "")
	require.NoError(t, err)

	c.AddUpstreamCaches(newContext(), uc)
	c.SetRecordAgeIgnoreTouch(0)
	c.SetCacheSignNarinfo(false)

	ni, err := c.GetNarInfo(context.Background(), testdata.Nar1.NarInfoHash)
	require.NoError(t, err)

	var found bool

	require.Len(t, ni.Signatures, 1, "must include our signature and the orignal one")

	var sig signature.Signature
	for _, sig = range ni.Signatures {
		if sig.Name == cacheName {
			found = true

			break
		}
	}

	assert.False(t, found)

	assert.False(t, signature.VerifyFirst(ni.Fingerprint(), ni.Signatures, []signature.PublicKey{c.PublicKey()}))
}

//nolint:paralleltest
func TestGetNarInfo(t *testing.T) {
	ts := testdata.NewTestServer(t, 40)
	defer ts.Close()

	dir, err := os.MkdirTemp("", "cache-path-")
	require.NoError(t, err)
	defer os.RemoveAll(dir) // clean up

	uc, err := upstream.New(newContext(), testhelper.MustParseURL(t, ts.URL), testdata.PublicKeys())
	require.NoError(t, err)

	dbFile := filepath.Join(dir, "var", "ncps", "db", "db.sqlite")
	testhelper.CreateMigrateDatabase(t, dbFile)

	db, err := database.Open("sqlite:" + dbFile)
	require.NoError(t, err)

	localStore, err := local.New(newContext(), dir)
	require.NoError(t, err)

	c, err := cache.New(newContext(), cacheName, db, localStore, localStore, localStore, "")
	require.NoError(t, err)

	c.AddUpstreamCaches(newContext(), uc)
	c.SetRecordAgeIgnoreTouch(0)

	t.Run("narinfo does not exist upstream", func(t *testing.T) {
		_, err := c.GetNarInfo(context.Background(), "doesnotexist")
		assert.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("narinfo exists upstream", func(t *testing.T) {
		t.Run("narinfo does not exist in storage yet", func(t *testing.T) {
			assert.NoFileExists(t, filepath.Join(dir, "store", "narinfo", testdata.Nar2.NarInfoPath))
		})

		t.Run("nar does not exist in storage yet", func(t *testing.T) {
			assert.NoFileExists(t, filepath.Join(dir, "store", "nar", testdata.Nar2.NarPath))
		})

		t.Run("narinfo does not exist in the database yet", func(t *testing.T) {
			rows, err := db.DB().Query("SELECT hash FROM narinfos")
			require.NoError(t, err)

			var hashes []string

			for rows.Next() {
				var hash string

				err := rows.Scan(&hash)
				require.NoError(t, err)

				hashes = append(hashes, hash)
			}

			require.NoError(t, rows.Err())
			assert.Empty(t, hashes)
		})

		t.Run("nar does not exist in the database yet", func(t *testing.T) {
			rows, err := db.DB().Query("SELECT hash FROM nars")
			require.NoError(t, err)

			var hashes []string

			for rows.Next() {
				var hash string

				err := rows.Scan(&hash)
				require.NoError(t, err)

				hashes = append(hashes, hash)
			}

			require.NoError(t, rows.Err())
			assert.Empty(t, hashes)
		})

		ni, err := c.GetNarInfo(context.Background(), testdata.Nar2.NarInfoHash)
		require.NoError(t, err)

		storePath := filepath.Join(dir, "store", "narinfo", testdata.Nar2.NarInfoPath)

		t.Run("size is correct", func(t *testing.T) {
			assert.Equal(t, uint64(50308), ni.FileSize)
		})

		t.Run("it should now exist in the store", func(t *testing.T) {
			assert.FileExists(t, storePath)
		})

		t.Run("it should be signed by our server", func(t *testing.T) {
			var found bool

			require.Len(t, ni.Signatures, 2, "must include our signature and the orignal one")

			var sig signature.Signature
			for _, sig = range ni.Signatures {
				if sig.Name == cacheName {
					found = true

					break
				}
			}

			assert.True(t, found)

			assert.True(t, signature.VerifyFirst(ni.Fingerprint(), ni.Signatures, []signature.PublicKey{c.PublicKey()}))
		})

		t.Run("it should not be signed twice by our server", func(t *testing.T) {
			ni, err := c.GetNarInfo(context.Background(), testdata.Nar2.NarInfoHash)
			require.NoError(t, err)

			require.Len(t, ni.Signatures, 2, "must include our signature and the orignal one")

			var sigs1 []signature.Signature

			for _, sig := range ni.Signatures {
				if sig.Name == cacheName {
					sigs1 = append(sigs1, sig)
				}
			}

			require.Len(t, sigs1, 1)

			idx := ts.AddMaybeHandler(func(w http.ResponseWriter, r *http.Request) bool {
				if r.URL.Path == "/"+testdata.Nar2.NarInfoHash+".narinfo" {
					_, err := w.Write([]byte(ni.String()))
					if err != nil {
						http.Error(w, err.Error(), http.StatusInternalServerError)
					}

					return true
				}

				return false
			})
			defer ts.RemoveMaybeHandler(idx)

			require.NoError(t, os.Remove(storePath))

			ni, err = c.GetNarInfo(context.Background(), testdata.Nar2.NarInfoHash)
			require.NoError(t, err)

			require.Len(t, ni.Signatures, 2, "must include our signature and the orignal one")

			var sigs2 []signature.Signature

			for _, sig := range ni.Signatures {
				if sig.Name == cacheName {
					sigs2 = append(sigs2, sig)
				}
			}

			require.Len(t, sigs2, 1)
		})

		t.Run("it should have also pulled the nar", func(t *testing.T) {
			// Force the other goroutine to run so it actually download the file
			// Try at least 10 times before announcing an error
			var err error

			for i := 0; i < 9; i++ {
				// NOTE: I tried runtime.Gosched() but it makes the test flaky
				time.Sleep(time.Millisecond)

				_, err = os.Stat(filepath.Join(dir, "store", "nar", testdata.Nar2.NarPath))
				if err == nil {
					break
				}
			}

			assert.NoError(t, err)
		})

		t.Run("narinfo does exist in the database, and has initial last_accessed_at", func(t *testing.T) {
			const query = `
			SELECT  hash, created_at,  last_accessed_at
			FROM narinfos
			`

			rows, err := db.DB().Query(query)
			require.NoError(t, err)

			nims := make([]database.NarInfo, 0)

			for rows.Next() {
				var nim database.NarInfo

				err := rows.Scan(&nim.Hash, &nim.CreatedAt, &nim.LastAccessedAt)
				require.NoError(t, err)

				nims = append(nims, nim)
			}

			require.NoError(t, rows.Err())

			assert.Len(t, nims, 1)
			assert.Equal(t, testdata.Nar2.NarInfoHash, nims[0].Hash)
			assert.Equal(t, nims[0].CreatedAt, nims[0].LastAccessedAt.Time)
		})

		t.Run("nar does exist in the database, and has initial last_accessed_at", func(t *testing.T) {
			const query = `
				SELECT  hash,  created_at,  last_accessed_at
				FROM nars
				`

			rows, err := db.DB().Query(query)
			require.NoError(t, err)

			nims := make([]database.Nar, 0)

			for rows.Next() {
				var nim database.Nar

				err := rows.Scan(
					&nim.Hash,
					&nim.CreatedAt,
					&nim.LastAccessedAt,
				)
				require.NoError(t, err)

				nims = append(nims, nim)
			}

			require.NoError(t, rows.Err())
			assert.Len(t, nims, 1)
			assert.Equal(t, testdata.Nar2.NarHash, nims[0].Hash)
			assert.Equal(t, nims[0].CreatedAt, nims[0].LastAccessedAt.Time)
		})

		t.Run("pulling it another time within recordAgeIgnoreTouch should not update last_accessed_at", func(t *testing.T) {
			time.Sleep(time.Second)

			c.SetRecordAgeIgnoreTouch(time.Hour)

			defer func() {
				c.SetRecordAgeIgnoreTouch(0)
			}()

			_, err := c.GetNarInfo(context.Background(), testdata.Nar2.NarInfoHash)
			require.NoError(t, err)

			t.Run("narinfo does exist in the database with the same last_accessed_at", func(t *testing.T) {
				const query = `
			SELECT  hash, created_at,  last_accessed_at
			FROM narinfos
			`

				rows, err := db.DB().Query(query)
				require.NoError(t, err)

				nims := make([]database.NarInfo, 0)

				for rows.Next() {
					var nim database.NarInfo

					err := rows.Scan(&nim.Hash, &nim.CreatedAt, &nim.LastAccessedAt)
					require.NoError(t, err)

					nims = append(nims, nim)
				}

				require.NoError(t, rows.Err())

				assert.Len(t, nims, 1)
				assert.Equal(t, testdata.Nar2.NarInfoHash, nims[0].Hash)
				assert.Equal(t, nims[0].CreatedAt, nims[0].LastAccessedAt.Time)
			})
		})

		t.Run("pulling it another time should update last_accessed_at only for narinfo", func(t *testing.T) {
			time.Sleep(time.Second)

			_, err := c.GetNarInfo(context.Background(), testdata.Nar2.NarInfoHash)
			require.NoError(t, err)

			t.Run("narinfo does exist in the database, and has more recent last_accessed_at", func(t *testing.T) {
				const query = `
			SELECT  hash, created_at,  last_accessed_at
			FROM narinfos
			`

				rows, err := db.DB().Query(query)
				require.NoError(t, err)

				nims := make([]database.NarInfo, 0)

				for rows.Next() {
					var nim database.NarInfo

					err := rows.Scan(&nim.Hash, &nim.CreatedAt, &nim.LastAccessedAt)
					require.NoError(t, err)

					nims = append(nims, nim)
				}

				require.NoError(t, rows.Err())

				assert.Len(t, nims, 1)
				assert.Equal(t, testdata.Nar2.NarInfoHash, nims[0].Hash)
				assert.NotEqual(t, nims[0].CreatedAt, nims[0].LastAccessedAt)
			})
		})

		t.Run("no error is returned if the entry already exist in the database", func(t *testing.T) {
			require.NoError(t, os.Remove(filepath.Join(dir, "store", "narinfo", testdata.Nar2.NarInfoPath)))

			_, err := c.GetNarInfo(context.Background(), testdata.Nar2.NarInfoHash)
			assert.NoError(t, err)
		})

		t.Run("nar does not exist in storage, it gets pulled automatically", func(t *testing.T) {
			narFile := filepath.Join(dir, "store", "nar", testdata.Nar2.NarPath)

			require.NoError(t, os.Remove(narFile))

			t.Run("it should not return an error", func(t *testing.T) {
				_, err := c.GetNarInfo(context.Background(), testdata.Nar2.NarInfoHash)
				assert.NoError(t, err)
			})

			t.Run("it should have also pulled the nar", func(t *testing.T) {
				// Force the other goroutine to run so it actually download the file
				// Try at least 10 times before announcing an error
				var err error

				for i := 0; i < 9; i++ {
					// NOTE: I tried runtime.Gosched() but it makes the test flaky
					time.Sleep(time.Millisecond)

					_, err = os.Stat(narFile)
					if err == nil {
						break
					}
				}

				assert.NoError(t, err)
			})
		})
	})

	t.Run("narinfo with transparent encryption", func(t *testing.T) {
		var allEntries []testdata.Entry

		for _, narEntry := range testdata.Entries {
			c := fmt.Sprintf("Compression: %s", narEntry.NarCompression)
			if !strings.Contains(narEntry.NarInfoText, c) {
				allEntries = append(allEntries, narEntry)
			}
		}

		for i, narEntry := range allEntries {
			t.Run("nar idx"+strconv.Itoa(i), func(t *testing.T) {
				narInfo, err := c.GetNarInfo(context.Background(), narEntry.NarInfoHash)
				require.NoError(t, err)

				if assert.Equal(t, nar.CompressionTypeZstd.String(), narInfo.Compression) {
					storePath := filepath.Join(dir, "store", "nar", narEntry.NarPath)
					if assert.FileExists(t, storePath) {
						body, err := os.ReadFile(storePath)
						require.NoError(t, err)

						if assert.NotEqual(t, narEntry.NarText, string(body), "returned body should be compressed") {
							decoder, err := zstd.NewReader(nil)
							require.NoError(t, err)

							plain, err := decoder.DecodeAll(body, []byte{})
							require.NoError(t, err)

							assert.Equal(t, narEntry.NarText, string(plain))

							//nolint:gosec
							assert.Len(t, body, int(narInfo.FileSize))
						}
					}
				}
			})
		}
	})
}

//nolint:paralleltest
func TestPutNarInfo(t *testing.T) {
	dir, err := os.MkdirTemp("", "cache-path-")
	require.NoError(t, err)
	defer os.RemoveAll(dir) // clean up

	dbFile := filepath.Join(dir, "var", "ncps", "db", "db.sqlite")
	testhelper.CreateMigrateDatabase(t, dbFile)

	db, err := database.Open("sqlite:" + dbFile)
	require.NoError(t, err)

	localStore, err := local.New(newContext(), dir)
	require.NoError(t, err)

	c, err := cache.New(newContext(), cacheName, db, localStore, localStore, localStore, "")
	require.NoError(t, err)

	c.SetRecordAgeIgnoreTouch(0)

	storePath := filepath.Join(dir, "store", "narinfo", testdata.Nar1.NarInfoPath)

	t.Run("narinfo does not exist in storage yet", func(t *testing.T) {
		assert.NoFileExists(t, storePath)
	})

	t.Run("narinfo does not exist in the database yet", func(t *testing.T) {
		rows, err := db.DB().Query("SELECT hash FROM narinfos")
		require.NoError(t, err)

		var hashes []string

		for rows.Next() {
			var hash string

			err := rows.Scan(&hash)
			require.NoError(t, err)

			hashes = append(hashes, hash)
		}

		require.NoError(t, rows.Err())
		assert.Empty(t, hashes)
	})

	t.Run("nar does not exist in the database yet", func(t *testing.T) {
		rows, err := db.DB().Query("SELECT hash FROM nars")
		require.NoError(t, err)

		var hashes []string

		for rows.Next() {
			var hash string

			err := rows.Scan(&hash)
			require.NoError(t, err)

			hashes = append(hashes, hash)
		}

		require.NoError(t, rows.Err())
		assert.Empty(t, hashes)
	})

	t.Run("PutNarInfo does not return an error", func(t *testing.T) {
		r := io.NopCloser(strings.NewReader(testdata.Nar1.NarInfoText))

		assert.NoError(t, c.PutNarInfo(context.Background(), testdata.Nar1.NarInfoHash, r))
	})

	t.Run("narinfo does exist in storage", func(t *testing.T) {
		assert.FileExists(t, storePath)
	})

	t.Run("it should be signed by our server", func(t *testing.T) {
		f, err := os.Open(storePath)
		require.NoError(t, err)

		ni, err := narinfo.Parse(f)
		require.NoError(t, err)

		var found bool

		var sig signature.Signature
		for _, sig = range ni.Signatures {
			if sig.Name == cacheName {
				found = true

				break
			}
		}

		assert.True(t, found)

		assert.True(t, signature.VerifyFirst(ni.Fingerprint(), ni.Signatures, []signature.PublicKey{c.PublicKey()}))
	})

	t.Run("narinfo does exist in the database", func(t *testing.T) {
		rows, err := db.DB().Query("SELECT hash FROM narinfos")
		require.NoError(t, err)

		var hashes []string

		for rows.Next() {
			var hash string

			err := rows.Scan(&hash)
			require.NoError(t, err)

			hashes = append(hashes, hash)
		}

		require.NoError(t, rows.Err())

		assert.Len(t, hashes, 1)
		assert.Equal(t, testdata.Nar1.NarInfoHash, hashes[0])
	})

	t.Run("nar does exist in the database", func(t *testing.T) {
		rows, err := db.DB().Query("SELECT hash FROM nars")
		require.NoError(t, err)

		var hashes []string

		for rows.Next() {
			var hash string

			err := rows.Scan(&hash)
			require.NoError(t, err)

			hashes = append(hashes, hash)
		}

		require.NoError(t, rows.Err())

		assert.Len(t, hashes, 1)
		assert.Equal(t, testdata.Nar1.NarHash, hashes[0])
	})
}

//nolint:paralleltest
func TestDeleteNarInfo(t *testing.T) {
	dir, err := os.MkdirTemp("", "cache-path-")
	require.NoError(t, err)
	defer os.RemoveAll(dir) // clean up

	dbFile := filepath.Join(dir, "var", "ncps", "db", "db.sqlite")
	testhelper.CreateMigrateDatabase(t, dbFile)

	db, err := database.Open("sqlite:" + dbFile)
	require.NoError(t, err)

	localStore, err := local.New(newContext(), dir)
	require.NoError(t, err)

	c, err := cache.New(newContext(), cacheName, db, localStore, localStore, localStore, "")
	require.NoError(t, err)

	c.SetRecordAgeIgnoreTouch(0)

	t.Run("file does not exist in the store", func(t *testing.T) {
		storePath := filepath.Join(dir, "store", "narinfo", testdata.Nar1.NarInfoPath)

		t.Run("narinfo does not exist in storage yet", func(t *testing.T) {
			assert.NoFileExists(t, storePath)
		})

		t.Run("DeleteNarInfo does return an error", func(t *testing.T) {
			err := c.DeleteNarInfo(context.Background(), testdata.Nar1.NarInfoHash)
			assert.ErrorIs(t, err, storage.ErrNotFound)
		})
	})

	t.Run("file does exist in the store", func(t *testing.T) {
		storePath := filepath.Join(dir, "store", "narinfo", testdata.Nar1.NarInfoPath)

		t.Run("narinfo does not exist in storage yet", func(t *testing.T) {
			assert.NoFileExists(t, storePath)
		})

		require.NoError(t, os.MkdirAll(filepath.Dir(storePath), 0o700))

		f, err := os.Create(storePath)
		require.NoError(t, err)

		_, err = f.WriteString(testdata.Nar1.NarInfoText)
		require.NoError(t, err)

		require.NoError(t, err)

		t.Run("narinfo does exist in storage", func(t *testing.T) {
			assert.FileExists(t, storePath)
		})

		t.Run("DeleteNarInfo does not return an error", func(t *testing.T) {
			assert.NoError(t, c.DeleteNarInfo(context.Background(), testdata.Nar1.NarInfoHash))
		})

		t.Run("narinfo is gone from the store", func(t *testing.T) {
			assert.NoFileExists(t, storePath)
		})
	})
}

//nolint:paralleltest
func TestGetNar(t *testing.T) {
	ts := testdata.NewTestServer(t, 40)
	defer ts.Close()

	dir, err := os.MkdirTemp("", "cache-path-")
	require.NoError(t, err)
	defer os.RemoveAll(dir) // clean up

	uc, err := upstream.New(newContext(), testhelper.MustParseURL(t, ts.URL), testdata.PublicKeys())
	require.NoError(t, err)

	dbFile := filepath.Join(dir, "var", "ncps", "db", "db.sqlite")
	testhelper.CreateMigrateDatabase(t, dbFile)

	db, err := database.Open("sqlite:" + dbFile)
	require.NoError(t, err)

	localStore, err := local.New(newContext(), dir)
	require.NoError(t, err)

	c, err := cache.New(newContext(), cacheName, db, localStore, localStore, localStore, "")
	require.NoError(t, err)

	c.AddUpstreamCaches(newContext(), uc)
	c.SetRecordAgeIgnoreTouch(0)

	t.Run("nar does not exist upstream", func(t *testing.T) {
		nu := nar.URL{Hash: "doesnotexist", Compression: nar.CompressionTypeXz}
		_, _, err := c.GetNar(context.Background(), nu)
		assert.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("nar exists upstream", func(t *testing.T) {
		t.Run("nar does not exist in storage yet", func(t *testing.T) {
			assert.NoFileExists(t, filepath.Join(dir, "store", "nar", testdata.Nar1.NarPath))
		})

		t.Run("nar does not exist in database yet", func(t *testing.T) {
			rows, err := db.DB().Query("SELECT hash FROM nars")
			require.NoError(t, err)

			var hashes []string

			for rows.Next() {
				var hash string

				err := rows.Scan(&hash)
				require.NoError(t, err)

				hashes = append(hashes, hash)
			}

			require.NoError(t, rows.Err())
			assert.Empty(t, hashes)
		})

		t.Run("getting the narinfo so the record in the database now exists", func(t *testing.T) {
			_, err := c.GetNarInfo(context.Background(), testdata.Nar1.NarInfoHash)
			assert.NoError(t, err)
		})

		nu := nar.URL{Hash: testdata.Nar1.NarHash, Compression: nar.CompressionTypeXz}
		size, r, err := c.GetNar(context.Background(), nu)
		require.NoError(t, err)

		defer r.Close()

		t.Run("size is correct", func(t *testing.T) {
			assert.Equal(t, int64(len(testdata.Nar1.NarText)), size)
		})

		t.Run("body is the same", func(t *testing.T) {
			body, err := io.ReadAll(r)
			require.NoError(t, err)

			if assert.Len(t, testdata.Nar1.NarText, len(string(body))) {
				assert.Equal(t, testdata.Nar1.NarText, string(body))
			}
		})

		t.Run("it should now exist in the store", func(t *testing.T) {
			assert.FileExists(t, filepath.Join(dir, "store", "nar", testdata.Nar1.NarPath))
		})

		t.Run("getting the narinfo so the record in the database now exists", func(t *testing.T) {
			_, err := c.GetNarInfo(context.Background(), testdata.Nar1.NarInfoHash)
			assert.NoError(t, err)
		})

		t.Run("nar does exist in the database, and has initial last_accessed_at", func(t *testing.T) {
			const query = `
				SELECT  hash,  created_at,  last_accessed_at
				FROM nars
				`

			rows, err := db.DB().Query(query)
			require.NoError(t, err)

			nims := make([]database.Nar, 0)

			for rows.Next() {
				var nim database.Nar

				err := rows.Scan(
					&nim.Hash,
					&nim.CreatedAt,
					&nim.LastAccessedAt,
				)
				require.NoError(t, err)

				nims = append(nims, nim)
			}

			require.NoError(t, rows.Err())

			assert.Len(t, nims, 1)
			assert.Equal(t, testdata.Nar1.NarHash, nims[0].Hash)
			assert.Equal(t, nims[0].CreatedAt, nims[0].LastAccessedAt.Time)
		})

		t.Run("pulling it another time within recordAgeIgnoreTouch should not update last_accessed_at", func(t *testing.T) {
			time.Sleep(time.Second)

			c.SetRecordAgeIgnoreTouch(time.Hour)

			defer func() {
				c.SetRecordAgeIgnoreTouch(0)
			}()

			nu := nar.URL{Hash: testdata.Nar1.NarHash, Compression: nar.CompressionTypeXz}

			_, r, err := c.GetNar(context.Background(), nu)
			require.NoError(t, err)
			defer r.Close()

			t.Run("narinfo does exist in the database with the same last_accessed_at", func(t *testing.T) {
				const query = `
				SELECT  hash,  created_at,  last_accessed_at
				FROM nars
				`

				rows, err := db.DB().Query(query)
				require.NoError(t, err)

				nims := make([]database.Nar, 0)

				for rows.Next() {
					var nim database.Nar

					err := rows.Scan(
						&nim.Hash,
						&nim.CreatedAt,
						&nim.LastAccessedAt,
					)
					require.NoError(t, err)

					nims = append(nims, nim)
				}

				require.NoError(t, rows.Err())

				assert.Len(t, nims, 1)
				assert.Equal(t, testdata.Nar1.NarHash, nims[0].Hash)
				assert.Equal(t, nims[0].CreatedAt, nims[0].LastAccessedAt.Time)
			})
		})

		t.Run("pulling it another time should update last_accessed_at", func(t *testing.T) {
			time.Sleep(time.Second)

			nu := nar.URL{Hash: testdata.Nar1.NarHash, Compression: nar.CompressionTypeXz}

			_, r, err := c.GetNar(context.Background(), nu)
			require.NoError(t, err)
			defer r.Close()

			t.Run("narinfo does exist in the database, and has more recent last_accessed_at", func(t *testing.T) {
				const query = `
				SELECT  hash,  created_at,  last_accessed_at
				FROM nars
				`

				rows, err := db.DB().Query(query)
				require.NoError(t, err)

				nims := make([]database.Nar, 0)

				for rows.Next() {
					var nim database.Nar

					err := rows.Scan(
						&nim.Hash,
						&nim.CreatedAt,
						&nim.LastAccessedAt,
					)
					require.NoError(t, err)

					nims = append(nims, nim)
				}

				require.NoError(t, rows.Err())

				assert.Len(t, nims, 1)
				assert.Equal(t, testdata.Nar1.NarHash, nims[0].Hash)
				assert.NotEqual(t, nims[0].CreatedAt, nims[0].LastAccessedAt)
			})
		})
	})
}

//nolint:paralleltest
func TestPutNar(t *testing.T) {
	dir, err := os.MkdirTemp("", "cache-path-")
	require.NoError(t, err)
	defer os.RemoveAll(dir) // clean up

	dbFile := filepath.Join(dir, "var", "ncps", "db", "db.sqlite")
	testhelper.CreateMigrateDatabase(t, dbFile)

	db, err := database.Open("sqlite:" + dbFile)
	require.NoError(t, err)

	localStore, err := local.New(newContext(), dir)
	require.NoError(t, err)

	c, err := cache.New(newContext(), cacheName, db, localStore, localStore, localStore, "")
	require.NoError(t, err)

	c.SetRecordAgeIgnoreTouch(0)

	storePath := filepath.Join(dir, "store", "nar", testdata.Nar1.NarPath)

	t.Run("nar does not exist in storage yet", func(t *testing.T) {
		assert.NoFileExists(t, storePath)
	})

	t.Run("putNar does not return an error", func(t *testing.T) {
		r := io.NopCloser(strings.NewReader(testdata.Nar1.NarText))

		nu := nar.URL{Hash: testdata.Nar1.NarHash, Compression: nar.CompressionTypeXz}
		_, err := c.PutNar(context.Background(), nu, r)
		assert.NoError(t, err)
	})

	t.Run("nar does exist in storage", func(t *testing.T) {
		f, err := os.Open(storePath)
		require.NoError(t, err)

		bs, err := io.ReadAll(f)
		require.NoError(t, err)

		assert.Equal(t, testdata.Nar1.NarText, string(bs))
	})
}

//nolint:paralleltest
func TestDeleteNar(t *testing.T) {
	dir, err := os.MkdirTemp("", "cache-path-")
	require.NoError(t, err)
	defer os.RemoveAll(dir) // clean up

	dbFile := filepath.Join(dir, "var", "ncps", "db", "db.sqlite")
	testhelper.CreateMigrateDatabase(t, dbFile)

	db, err := database.Open("sqlite:" + dbFile)
	require.NoError(t, err)

	localStore, err := local.New(newContext(), dir)
	require.NoError(t, err)

	c, err := cache.New(newContext(), cacheName, db, localStore, localStore, localStore, "")
	require.NoError(t, err)

	c.SetRecordAgeIgnoreTouch(0)

	storePath := filepath.Join(dir, "store", "nar", testdata.Nar1.NarPath)

	t.Run("file does not exist in the store", func(t *testing.T) {
		t.Run("nar does not exist in storage yet", func(t *testing.T) {
			assert.NoFileExists(t, storePath)
		})

		t.Run("DeleteNar does return an error", func(t *testing.T) {
			nu := nar.URL{Hash: testdata.Nar1.NarHash, Compression: nar.CompressionTypeXz}
			err := c.DeleteNar(context.Background(), nu)
			assert.ErrorIs(t, err, storage.ErrNotFound)
		})
	})

	t.Run("file does exist in the store", func(t *testing.T) {
		t.Run("nar does not exist in storage yet", func(t *testing.T) {
			assert.NoFileExists(t, storePath)
		})

		require.NoError(t, os.MkdirAll(filepath.Dir(storePath), 0o700))

		f, err := os.Create(storePath)
		require.NoError(t, err)

		_, err = f.WriteString(testdata.Nar1.NarText)
		require.NoError(t, err)

		require.NoError(t, f.Close())

		t.Run("nar does exist in storage", func(t *testing.T) {
			assert.FileExists(t, storePath)
		})

		t.Run("deleteNar does not return an error", func(t *testing.T) {
			nu := nar.URL{Hash: testdata.Nar1.NarHash, Compression: nar.CompressionTypeXz}
			err := c.DeleteNar(context.Background(), nu)
			assert.NoError(t, err)
		})

		t.Run("nar is gone from the store", func(t *testing.T) {
			assert.NoFileExists(t, storePath)
		})
	})
}

//nolint:funlen,cyclop
func TestGetNar_StreamingScenarios(t *testing.T) {
	t.Parallel()

	narHash := "testnarhash"
	narContent := "This is a test NAR content."
	narURL := nar.URL{Hash: narHash, Compression: nar.CompressionTypeNone}

	t.Run("NAR not in store, successful stream and store", func(t *testing.T) {
		t.Parallel()

		ctx := newContext()
		upstreamServer := testdata.NewTestServer(t, 1)
		defer upstreamServer.Close()
		cacheInstance, mockNarStoreInstance := setupTestCacheWithMocks(t, ctx, upstreamServer)

		// Mock the upstream server to provide the nar and narinfo
		narinfoText := fmt.Sprintf("StorePath: /nix/store/%s-test\nURL: nar/%s.nar", narHash, narHash)
		upstreamServer.AddMaybeHandler(func(w http.ResponseWriter, r *http.Request) bool {
			if strings.Contains(r.URL.Path, narHash+".narinfo") {
				_, _ = io.WriteString(w, narinfoText)
				return true
			}
			if strings.Contains(r.URL.Path, narHash+".nar") {
				w.Header().Set("Content-Length", strconv.Itoa(len(narContent)))
				_, _ = io.WriteString(w, narContent)
				return true
			}
			return false
		})

		mockNarStoreInstance.hasNarFunc = func(ctx context.Context, nu nar.URL) bool {
			return false // Not in store initially
		}

		size, reader, err := cacheInstance.GetNar(ctx, narURL)
		require.NoError(t, err)
		require.NotNil(t, reader)
		defer reader.Close()

		readBytes, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, narContent, string(readBytes))
		assert.Equal(t, int64(len(narContent)), size)
	})

	t.Run("NAR not in store, upstream returns 404", func(t *testing.T) {
		t.Parallel()
		ctx := newContext()
		upstreamServer := testdata.NewTestServer(t, 1)
		defer upstreamServer.Close()
		cacheInstance, _ := setupTestCacheWithMocks(t, ctx, upstreamServer)

		upstreamServer.AddMaybeHandler(func(w http.ResponseWriter, r *http.Request) bool {
			if strings.Contains(r.URL.Path, narHash) {
				http.Error(w, "Not Found", http.StatusNotFound)
				return true
			}
			return false
		})

		_, _, err := cacheInstance.GetNar(ctx, narURL)
		assert.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("NAR not in store, upstream returns 500", func(t *testing.T) {
		t.Parallel()
		ctx := newContext()
		upstreamServer := testdata.NewTestServer(t, 1)
		defer upstreamServer.Close()
		cacheInstance, mockNarStoreInstance := setupTestCacheWithMocks(t, ctx, upstreamServer)

		upstreamServer.AddMaybeHandler(func(w http.ResponseWriter, r *http.Request) bool {
			if strings.Contains(r.URL.Path, narHash) {
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return true
			}
			return false
		})
		mockNarStoreInstance.hasNarFunc = func(ctx context.Context, nu nar.URL) bool { return false }

		_, _, err := cacheInstance.GetNar(ctx, narURL)
		assert.Error(t, err) // We expect an error, not necessarily storage.ErrNotFound
		// Specific error might be upstream.ErrUnexpectedHTTPStatusCode or similar, wrapped.
		// Ensure PutNar was not called
		assert.Len(t, mockNarStoreInstance.GetPutNarCalls(), 0)
	})

	t.Run("NAR not in store, error during narStore.PutNar", func(t *testing.T) {
		t.Parallel()
		ctx := newContext()
		upstreamServer := testdata.NewTestServer(t, 1)
		defer upstreamServer.Close()
		cacheInstance, mockNarStoreInstance := setupTestCacheWithMocks(t, ctx, upstreamServer)
		putNarError := errors.New("simulated PutNar error (disk full)")

		upstreamServer.AddMaybeHandler(func(w http.ResponseWriter, r *http.Request) bool {
			if strings.Contains(r.URL.Path, narHash) {
				w.Header().Set("Content-Length", strconv.Itoa(len(narContent)))
				_, err := io.WriteString(w, narContent)
				require.NoError(t, err)
				return true
			}
			return false
		})

		mockNarStoreInstance.hasNarFunc = func(ctx context.Context, nu nar.URL) bool { return false }
		mockNarStoreInstance.putNarFunc = func(ctx context.Context, nu nar.URL, r io.Reader) (int64, error) {
			// Consume reader to allow client part to proceed
			// written, _ := io.Copy(io.Discard, r) // This is now done by the mock wrapper
			// return written, putNarError
			return 0, putNarError
		}

		size, reader, err := cacheInstance.GetNar(ctx, narURL)
		require.NoError(t, err) // GetNar itself shouldn't error here, client gets the stream
		require.NotNil(t, reader)
		defer reader.Close()

		// Client should still be able to read the content
		readBytes, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, narContent, string(readBytes))
		assert.Equal(t, int64(len(narContent)), size)

		// Check that PutNar was attempted
		// Need a short delay for the goroutine executing PutNar to run and error out.
		time.Sleep(100 * time.Millisecond)
		assert.Len(t, mockNarStoreInstance.GetPutNarCalls(), 1)

		// Verify NAR is not considered "in store" afterwards by HasNar (if PutNar failed)
		// This depends on how HasNar is implemented. For this mock, we'll set it.
		mockNarStoreInstance.hasNarFunc = func(ctx context.Context, nu nar.URL) bool { return false }
		_, _, err = cacheInstance.GetNar(ctx, narURL) // Try fetching again
		// This second fetch will again go to upstream because PutNar failed.
		// To properly test this, we'd need to check upstream was hit twice.
		// For now, we confirmed the first stream succeeded for the client despite PutNar error.
		require.Error(t, err) // This will fail if upstream is not set to respond again.
		// This part of the test needs refinement on asserting "not in store" state.
		// The key is the client got the content on the first try.
	})

	t.Run("NAR already in store", func(t *testing.T) {
		t.Parallel()
		ctx := newContext()
		upstreamServer := testdata.NewTestServer(t, 1)
		defer upstreamServer.Close()
		cacheInstance, mockNarStoreInstance := setupTestCacheWithMocks(t, ctx, upstreamServer)

		mockNarStoreInstance.hasNarFunc = func(ctx context.Context, nu nar.URL) bool {
			return nu.Hash == narHash
		}
		mockNarStoreInstance.getNarFunc = func(ctx context.Context, nu nar.URL) (int64, io.ReadCloser, error) {
			if nu.Hash == narHash {
				return int64(len(narContent)), io.NopCloser(strings.NewReader(narContent)), nil
			}
			return 0, nil, storage.ErrNotFound
		}

		size, reader, err := cacheInstance.GetNar(ctx, narURL)
		require.NoError(t, err)
		require.NotNil(t, reader)
		defer reader.Close()

		readBytes, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, narContent, string(readBytes))
		assert.Equal(t, int64(len(narContent)), size)

		// Ensure PutNar was not called
		assert.Len(t, mockNarStoreInstance.GetPutNarCalls(), 0)
		// Ensure upstream was not called (by checking test server's request count if possible, or handler calls)
		// testdata.TestServer doesn't directly expose request counts easily.
		// Rely on PutNarCalls and the logic: if HasNar is true, upstream path is skipped.
	})
}

func TestGetNar_StreamingConcurrentRequests(t *testing.T) {
	t.Parallel()

	ctx := newContext()
	narHash := "concurrentNarHash"
	narContent := "concurrent NAR content for streaming"
	narURL := nar.URL{Hash: narHash, Compression: nar.CompressionTypeNone}

	upstreamServer := testdata.NewTestServer(t, 1) // Expect only one actual fetch from upstream
	defer upstreamServer.Close()
	upstreamCacheImpl, err := upstream.New(ctx, testhelper.MustParseURL(t, upstreamServer.URL), nil)
	require.NoError(t, err)

	dbDir, err := os.MkdirTemp("", "cache-db-conc-")
	require.NoError(t, err)
	defer os.RemoveAll(dbDir)
	dbFile := filepath.Join(dbDir, "db.sqlite")
	testhelper.CreateMigrateDatabase(t, dbFile)
	db, err := database.Open("sqlite:" + dbFile)
	require.NoError(t, err)

	mockNarStoreInstance := &mockNarStore{}

	storeDir, err := os.MkdirTemp("", "ncps-store-conc-")
	require.NoError(t, err)
	defer os.RemoveAll(storeDir)
	simpleLocalStore, err := local.New(ctx, storeDir)
	require.NoError(t, err)

	cacheInstance, err := cache.New(ctx, cacheName, db, simpleLocalStore, simpleLocalStore, mockNarStoreInstance, "")
	require.NoError(t, err)
	cacheInstance.AddUpstreamCaches(ctx, upstreamCacheImpl)
	cacheInstance.SetRecordAgeIgnoreTouch(0)

	// --- Test Setup ---
	// Upstream handler: serves content once, then records subsequent unexpected calls.
	var upstreamFetchCount int32
	upstreamServer.AddMaybeHandler(func(w http.ResponseWriter, r *http.Request) bool {
		if strings.Contains(r.URL.Path, narHash) {
			currentFetchCount := atomic.AddInt32(&upstreamFetchCount, 1)
			if currentFetchCount > 1 {
				// This should not happen if logic is correct
				t.Errorf("Upstream cache fetched %d times, expected only once", currentFetchCount)
				http.Error(w, "Upstream fetched too many times", http.StatusInternalServerError)
				return true
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(narContent)))
			time.Sleep(200 * time.Millisecond) // Simulate some network delay for the first fetch
			_, err := io.WriteString(w, narContent)
			require.NoError(t, err)
			return true
		}
		return false
	})

	// Mock NarStore behavior:
	// Initially, NAR is not in store.
	// After the first successful PutNar, it will be "in store".
	var narStoredSuccessfully sync.Once
	var muNarStoreState sync.Mutex // Protects access to hasNarState and getNarContent
	hasNarState := false
	var getNarContent string

	mockNarStoreInstance.hasNarFunc = func(ctx context.Context, nu nar.URL) bool {
		muNarStoreState.Lock()
		defer muNarStoreState.Unlock()
		if nu.Hash == narHash {
			return hasNarState
		}
		return false
	}

	mockNarStoreInstance.putNarFunc = func(ctx context.Context, nu nar.URL, r io.Reader) (int64, error) {
		// The mockNarStore's main PutNar already copies to io.Discard.
		// We just need to signal that it's "stored" and capture the content.
		// This function is called *after* the content is read from r by the mock's io.Copy.
		// So, we can't read 'r' here again.
		// To capture content for GetNar, we'd need to intercept earlier or have TeeReader in mock.
		// For simplicity, we'll assume narContent is stored.
		narStoredSuccessfully.Do(func() {
			muNarStoreState.Lock()
			hasNarState = true
			getNarContent = narContent // Simulate content being stored
			muNarStoreState.Unlock()
			t.Logf("Simulated PutNar successful for %s", nu.Hash)
		})
		return int64(len(narContent)), nil
	}

	mockNarStoreInstance.getNarFunc = func(ctx context.Context, nu nar.URL) (int64, io.ReadCloser, error) {
		muNarStoreState.Lock()
		defer muNarStoreState.Unlock()
		if nu.Hash == narHash && hasNarState {
			return int64(len(getNarContent)), io.NopCloser(strings.NewReader(getNarContent)), nil
		}
		return 0, nil, storage.ErrNotFound
	}

	// --- Concurrency Test ---
	numRequests := 5
	var wg sync.WaitGroup
	errs := make(chan error, numRequests)

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(reqNum int) {
			defer wg.Done()
			t.Logf("Goroutine %d: Requesting NAR", reqNum)
			size, reader, err := cacheInstance.GetNar(ctx, narURL)
			if err != nil {
				t.Errorf("Goroutine %d: GetNar returned error: %v", reqNum, err)
				errs <- err
				return
			}
			defer reader.Close()

			readBytes, err := io.ReadAll(reader)
			if err != nil {
				t.Errorf("Goroutine %d: Error reading NAR content: %v", reqNum, err)
				errs <- err
				return
			}
			if string(readBytes) != narContent {
				t.Errorf("Goroutine %d: Content mismatch. Got %q, want %q", reqNum, string(readBytes), narContent)
				errs <- fmt.Errorf("content mismatch for goroutine %d", reqNum)
				return
			}
			if size != int64(len(narContent)) {
				t.Errorf("Goroutine %d: Size mismatch. Got %d, want %d", reqNum, size, int64(len(narContent)))
				errs <- fmt.Errorf("size mismatch for goroutine %d", reqNum)
				return
			}
			t.Logf("Goroutine %d: Successfully read NAR", reqNum)
			errs <- nil
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	// Assertions after all goroutines are done
	finalUpstreamCount := atomic.LoadInt32(&upstreamFetchCount)
	assert.Equal(t, int32(1), finalUpstreamCount, "Upstream cache should only be fetched once")

	putCalls := mockNarStoreInstance.GetPutNarCalls()
	assert.Len(t, putCalls, 1, "PutNar should only be called once")
	if len(putCalls) > 0 {
		assert.Equal(t, narHash, putCalls[0].Hash)
	}

	muNarStoreState.Lock()
	assert.True(t, hasNarState, "NAR should be marked as stored in mockNarStore")
	muNarStoreState.Unlock()
}
