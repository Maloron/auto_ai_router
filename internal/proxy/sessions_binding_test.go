package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mixaill76/auto_ai_router/internal/config"
	"github.com/mixaill76/auto_ai_router/internal/ratelimit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func stickyRouteRequest(ctx context.Context, sessionID string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(
		`{"model":"route-a","messages":[{"role":"user","content":"hi"}],"user":"`+sessionID+`"}`,
	)).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer master-key")
	req.Header.Set("Content-Type", "application/json")
	return req
}

func okProvider(t *testing.T, calls *atomic.Int32) *httptest.Server {
	t.Helper()
	return newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(chatCompletionOKBody("id-ok", "ok")))
	}))
}

// A bound credential that fails over keeps the binding: the next turn goes back
// to it (and its prompt cache) instead of staying on the substitute.
func TestStickyBinding_KeptAfterFailoverFromBoundCredential(t *testing.T) {
	var failingCalls, okCalls atomic.Int32
	failing := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		failingCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"message":"failed"}}`))
	}))
	defer failing.Close()
	ok := okProvider(t, &okCalls)
	defer ok.Close()

	bound := directDenylistCredential("bound-provider", failing.URL)
	other := directDenylistCredential("other-provider", ok.URL)
	prx := NewTestProxyBuilder().
		WithCredentials(bound, other).
		WithMasterKey("master-key").
		WithMaxProviderRetries(1).
		WithSessionSticky(5 * time.Minute).
		Build()
	registerTestModel(prx, bound.Name, "route-a")
	registerTestModel(prx, other.Name, "route-a")
	prx.sessionStore.Set("sess-1", "route-a", bound.Name)

	w := httptest.NewRecorder()
	prx.ProxyRequest(w, stickyRouteRequest(context.Background(), "sess-1"))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, int32(1), failingCalls.Load())
	assert.Equal(t, int32(1), okCalls.Load())
	cred, found := prx.sessionStore.Get("sess-1", "route-a")
	require.True(t, found, "binding must survive a failover away from the bound credential")
	assert.Equal(t, bound.Name, cred)
}

// A bound credential that is banned for a moment is skipped for this turn but
// the session stays bound to it.
func TestStickyBinding_HeldWhileBoundCredentialBanned(t *testing.T) {
	var boundCalls, otherCalls atomic.Int32
	boundSrv := okProvider(t, &boundCalls)
	defer boundSrv.Close()
	otherSrv := okProvider(t, &otherCalls)
	defer otherSrv.Close()

	bound := directDenylistCredential("bound-provider", boundSrv.URL)
	other := directDenylistCredential("other-provider", otherSrv.URL)
	prx := NewTestProxyBuilder().
		WithCredentials(bound, other).
		WithMasterKey("master-key").
		WithSessionSticky(5 * time.Minute).
		Build()
	registerTestModel(prx, bound.Name, "route-a")
	registerTestModel(prx, other.Name, "route-a")
	prx.sessionStore.Set("sess-2", "route-a", bound.Name)
	prx.balancer.BanUntil(bound.Name, "route-a", http.StatusTooManyRequests, time.Now().Add(time.Minute), "test")

	w := httptest.NewRecorder()
	prx.ProxyRequest(w, stickyRouteRequest(context.Background(), "sess-2"))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Zero(t, boundCalls.Load())
	assert.Equal(t, int32(1), otherCalls.Load())
	cred, found := prx.sessionStore.Get("sess-2", "route-a")
	require.True(t, found)
	assert.Equal(t, bound.Name, cred, "a short ban must not re-home the session")
}

// A binding to a credential that no longer serves the model moves to the one
// that served the turn.
func TestStickyBinding_MovesWhenBoundCredentialNoLongerServesModel(t *testing.T) {
	var calls atomic.Int32
	srv := okProvider(t, &calls)
	defer srv.Close()

	only := directDenylistCredential("only-provider", srv.URL)
	prx := NewTestProxyBuilder().
		WithCredentials(only).
		WithMasterKey("master-key").
		WithSessionSticky(5 * time.Minute).
		Build()
	registerTestModel(prx, only.Name, "route-a")
	prx.sessionStore.Set("sess-3", "route-a", "removed-provider")

	w := httptest.NewRecorder()
	prx.ProxyRequest(w, stickyRouteRequest(context.Background(), "sess-3"))

	require.Equal(t, http.StatusOK, w.Code)
	cred, found := prx.sessionStore.Get("sess-3", "route-a")
	require.True(t, found)
	assert.Equal(t, only.Name, cred)
}

// A client that hangs up says nothing about the credential: the binding stays.
func TestStickyBinding_KeptWhenClientCancels(t *testing.T) {
	var calls atomic.Int32
	srv := okProvider(t, &calls)
	defer srv.Close()

	bound := directDenylistCredential("bound-provider", srv.URL)
	prx := NewTestProxyBuilder().
		WithCredentials(bound).
		WithMasterKey("master-key").
		WithSessionSticky(5 * time.Minute).
		Build()
	registerTestModel(prx, bound.Name, "route-a")
	prx.sessionStore.Set("sess-4", "route-a", bound.Name)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := httptest.NewRecorder()
	prx.ProxyRequest(w, stickyRouteRequest(ctx, "sess-4"))

	cred, found := prx.sessionStore.Get("sess-4", "route-a")
	require.True(t, found, "client cancellation must not clear the session binding")
	assert.Equal(t, bound.Name, cred)
}

// ── Redis-backed store (integration, needs VALKEY_ADDR) ──────────────────────

func redisSessionStoresForTest(t *testing.T, ttl time.Duration) (*redisSessionStore, *redisSessionStore) {
	t.Helper()
	addr := os.Getenv("VALKEY_ADDR")
	if addr == "" {
		t.Skip("VALKEY_ADDR not set, skipping Redis integration test")
	}
	client, err := ratelimit.NewValkeyClient(config.RedisConfig{InitAddresses: []string{addr}, ForceSingleClient: true})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	prefix := "test-sticky-" + t.Name() + ":"
	return newRedisSessionStore(client, prefix, ttl, nil), newRedisSessionStore(client, prefix, ttl, nil)
}

func TestRedisSessionStore_SharedBetweenReplicas(t *testing.T) {
	replicaA, replicaB := redisSessionStoresForTest(t, time.Minute)

	replicaA.Set("sess", "model-a", "cred-1")
	cred, found := replicaB.Get("sess", "model-a")
	require.True(t, found, "a binding written by one replica must be visible to another")
	assert.Equal(t, "cred-1", cred)

	replicaB.Delete("sess", "model-a")
	_, found = replicaA.Get("sess", "model-a")
	assert.False(t, found)
}

func TestRedisSessionStore_Expires(t *testing.T) {
	store, _ := redisSessionStoresForTest(t, 50*time.Millisecond)
	store.Set("sess", "model-a", "cred-1")
	time.Sleep(120 * time.Millisecond)
	_, found := store.Get("sess", "model-a")
	assert.False(t, found)
}

func TestRedisSessionStore_KeyHashesSessionID(t *testing.T) {
	s := &redisSessionStore{keyPrefix: "ru02:"}
	key := s.key(strings.Repeat("x", 10_000), "model-a")
	assert.True(t, strings.HasPrefix(key, "ru02:session:"))
	assert.Len(t, key, len("ru02:session:")+32)
	assert.NotEqual(t, s.key("a", "model-a"), s.key("a", "model-b"))
}
