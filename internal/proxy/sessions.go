package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valkey-io/valkey-go"
)

// sessionBindingStore keeps session -> credential bindings for session-sticky
// routing. The in-memory SessionStore is local to one router replica; the
// Redis-backed store shares bindings between all replicas of a deployment, so a
// follow-up turn that lands on another pod still reaches the same credential
// (and the provider's prompt cache on it).
type sessionBindingStore interface {
	Get(sessionID, modelID string) (string, bool)
	Set(sessionID, modelID, credentialName string)
	Delete(sessionID, modelID string)
	Len() int
	StartCleanup(ctx context.Context, interval time.Duration)
}

type sessionKey struct {
	sessionID string
	modelID   string
}

type SessionEntry struct {
	CredentialName string
	LastAccess     time.Time
}

// SessionStore is the process-local binding store.
//
// The TTL is refreshed by Set only, i.e. when the bound credential actually
// served a turn. Get does not extend it: a binding whose credential keeps
// being unavailable expires after one TTL instead of living forever.
type SessionStore struct {
	mu      sync.Mutex
	entries map[sessionKey]*SessionEntry
	ttl     time.Duration
	now     func() time.Time
}

func NewSessionStore(ttl time.Duration) *SessionStore {
	return &SessionStore{
		entries: make(map[sessionKey]*SessionEntry),
		ttl:     ttl,
		now:     time.Now,
	}
}

func (s *SessionStore) Get(sessionID, modelID string) (string, bool) {
	key := sessionKey{sessionID: sessionID, modelID: modelID}

	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.entries[key]
	if !ok {
		return "", false
	}

	if s.now().Sub(entry.LastAccess) > s.ttl {
		delete(s.entries, key)
		return "", false
	}

	return entry.CredentialName, true
}

func (s *SessionStore) Set(sessionID, modelID, credentialName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.entries[sessionKey{sessionID: sessionID, modelID: modelID}] = &SessionEntry{
		CredentialName: credentialName,
		LastAccess:     s.now(),
	}
}

func (s *SessionStore) Delete(sessionID, modelID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, sessionKey{sessionID: sessionID, modelID: modelID})
}

func (s *SessionStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpiredLocked(s.now())
	return len(s.entries)
}

func (s *SessionStore) StartCleanup(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			s.cleanupExpiredLocked(s.now())
			s.mu.Unlock()
		}
	}
}

func (s *SessionStore) cleanupExpiredLocked(now time.Time) {
	for key, entry := range s.entries {
		if now.Sub(entry.LastAccess) > s.ttl {
			delete(s.entries, key)
		}
	}
}

// redisSessionCommandTimeout bounds every binding lookup/update so a slow Redis
// costs at most this much on the request path; on timeout or error the store
// falls back to the replica-local copy.
const redisSessionCommandTimeout = 200 * time.Millisecond

// redisSessionStore shares bindings through Redis/Valkey (keys
// "{prefix}session:{sha256(model, session)}", native EX TTL) and mirrors every
// write into a local SessionStore used when Redis is unreachable.
type redisSessionStore struct {
	client    valkey.Client
	keyPrefix string
	ttl       time.Duration
	local     *SessionStore
	logger    *slog.Logger
	warned    atomic.Bool
}

func newRedisSessionStore(client valkey.Client, keyPrefix string, ttl time.Duration, logger *slog.Logger) *redisSessionStore {
	return &redisSessionStore{
		client:    client,
		keyPrefix: keyPrefix,
		ttl:       ttl,
		local:     NewSessionStore(ttl),
		logger:    logger,
	}
}

// key hashes the client-supplied session ID so key length and charset do not
// depend on what clients send.
func (s *redisSessionStore) key(sessionID, modelID string) string {
	sum := sha256.Sum256([]byte(modelID + "\x00" + sessionID))
	return s.keyPrefix + "session:" + hex.EncodeToString(sum[:16])
}

func (s *redisSessionStore) warn(op string, err error) {
	if s.logger != nil && s.warned.CompareAndSwap(false, true) {
		s.logger.Warn("Session-sticky routing: Redis unavailable, using replica-local bindings",
			"operation", op, "error", err)
	}
}

func (s *redisSessionStore) Get(sessionID, modelID string) (string, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), redisSessionCommandTimeout)
	defer cancel()
	cred, err := s.client.Do(ctx, s.client.B().Get().Key(s.key(sessionID, modelID)).Build()).ToString()
	if err == nil {
		s.warned.Store(false)
		return cred, cred != ""
	}
	if valkey.IsValkeyNil(err) {
		return "", false
	}
	s.warn("get", err)
	return s.local.Get(sessionID, modelID)
}

func (s *redisSessionStore) Set(sessionID, modelID, credentialName string) {
	s.local.Set(sessionID, modelID, credentialName)
	ctx, cancel := context.WithTimeout(context.Background(), redisSessionCommandTimeout)
	defer cancel()
	cmd := s.client.B().Set().Key(s.key(sessionID, modelID)).Value(credentialName).Px(s.ttl).Build()
	if err := s.client.Do(ctx, cmd).Error(); err != nil {
		s.warn("set", err)
	}
}

func (s *redisSessionStore) Delete(sessionID, modelID string) {
	s.local.Delete(sessionID, modelID)
	ctx, cancel := context.WithTimeout(context.Background(), redisSessionCommandTimeout)
	defer cancel()
	if err := s.client.Do(ctx, s.client.B().Del().Key(s.key(sessionID, modelID)).Build()).Error(); err != nil {
		s.warn("delete", err)
	}
}

// Len reports the replica-local mirror; the shared Redis keyspace is not scanned.
func (s *redisSessionStore) Len() int { return s.local.Len() }

func (s *redisSessionStore) StartCleanup(ctx context.Context, interval time.Duration) {
	s.local.StartCleanup(ctx, interval)
}
