package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mixaill76/auto_ai_router/internal/config"
	"github.com/mixaill76/auto_ai_router/internal/litellmdb"
	dbmodels "github.com/mixaill76/auto_ai_router/internal/litellmdb/models"
	routermodels "github.com/mixaill76/auto_ai_router/internal/models"
	"github.com/mixaill76/auto_ai_router/internal/testhelpers"
	"github.com/mixaill76/auto_ai_router/internal/video"
	"github.com/stretchr/testify/require"
)

type videoSpendCommitter struct {
	entries []*dbmodels.SpendLogEntry
}

func TestVideoPrincipalResolverUsesOrganizationPolicyPrice(t *testing.T) {
	pricePath := filepath.Join(t.TempDir(), "prices.json")
	require.NoError(t, os.WriteFile(pricePath, []byte(`{"runway/gen4_turbo":{"output_cost_per_video_per_second":0.07}}`), 0600))
	manager := routermodels.New(testhelpers.NewTestLogger(), 100, nil)
	manager.SetClientModelIDs([]string{"runway/gen4_turbo"})
	manager.SetExternalModelIDs([]string{"runway/gen4_turbo"})
	policies, err := routermodels.LoadOrganizationPolicies([]config.OrganizationPolicyConfig{{
		OrganizationID:  "org-1",
		PriceProfileID:  "r8",
		ModelPricesLink: pricePath,
		AllowlistSet:    true,
		ModelAllowlist:  []string{"runway/gen4_turbo"},
	}}, manager, routermodels.OrganizationPolicyLoadOptions{
		LiteLLMDBEnabled: true, LiteLLMDBRequired: true,
	})
	require.NoError(t, err)
	db := &organizationPolicyTestDB{tokens: map[string]*dbmodels.TokenInfo{
		"video-key": {
			Token: "video-key-hash", UserID: "user-1", TeamID: "team-1",
			DirectOrganizationID: "org-1", OrganizationID: "org-1",
		},
	}}
	builder := NewTestProxyBuilder().WithMasterKey("master-key")
	builder.config.ModelManager = manager
	builder.config.OrganizationPolicies = policies
	prx := builder.Build()
	prx.LiteLLMDB = db

	request := httptest.NewRequest(http.MethodPost, "/v1/videos", nil)
	request.Header.Set("Authorization", "Bearer video-key")
	response := httptest.NewRecorder()
	principal, err := NewVideoPrincipalResolver(prx).ResolvePrincipal(response, request, "runway/gen4_turbo")
	require.NoError(t, err)
	require.Equal(t, "org-1", principal.OrganizationID)
	require.Equal(t, "video-key-hash", principal.APIKeyHash)
	require.Equal(t, "r8", principal.PriceProfileID)
	require.Equal(t, "0.07", principal.RatePerSecond)
	require.Equal(t, "USD", principal.Currency)

	limit := 10.0
	db.tokens["video-key"].MaxBudget = &limit
	limitedRequest := httptest.NewRequest(http.MethodPost, "/v1/videos", nil)
	limitedRequest.Header.Set("Authorization", "Bearer video-key")
	limitedResponse := httptest.NewRecorder()
	_, err = NewVideoPrincipalResolver(prx).ResolvePrincipal(limitedResponse, limitedRequest, "runway/gen4_turbo")
	require.Error(t, err)
	require.Equal(t, http.StatusServiceUnavailable, limitedResponse.Code)
}

func (c *videoSpendCommitter) CommitSpend(_ context.Context, entry *dbmodels.SpendLogEntry) (litellmdb.SpendCommitResult, error) {
	c.entries = append(c.entries, entry)
	return litellmdb.SpendCommitResult{Inserted: len(c.entries) == 1, EffectiveRequestID: entry.RequestID}, nil
}

func TestVideoBillingUsesStableSpendIdentityAndOrganizationProfile(t *testing.T) {
	committer := new(videoSpendCommitter)
	billing := NewVideoBilling(committer)
	job := &video.Job{
		ID:           "vid-stable",
		CreatedAt:    time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC),
		QuotedAmount: "0.35",
		Currency:     "USD",
		Request: video.CreateRequest{
			Model:           "runway/gen4_turbo",
			DurationSeconds: 5,
		},
		Principal: video.Principal{
			OrganizationID:     "org-1",
			APIKeyHash:         "key-hash",
			UserID:             "user-1",
			TeamID:             "team-1",
			BillingTeamID:      "team-1",
			PriceProfileID:     "cloud-ru-usd-2026-07-15-r8",
			PriceProfileSHA256: "profile-sha",
			RatePerSecond:      "0.07",
			Currency:           "USD",
		},
	}

	require.NoError(t, billing.Settle(t.Context(), job, job.ID+":settle"))
	require.NoError(t, billing.Settle(t.Context(), job, job.ID+":settle"))
	require.Len(t, committer.entries, 2)
	for _, entry := range committer.entries {
		require.Equal(t, job.ID, entry.RequestID)
		require.Equal(t, job.ID, entry.AirEventID)
		require.Equal(t, "avideo_generation", entry.CallType)
		require.Equal(t, 0.35, entry.Spend)
		require.Equal(t, "org-1", entry.OrganizationID)
		require.Equal(t, "key-hash", entry.APIKey)
		require.Equal(t, "team-1", entry.BillingTeamID)
		var metadata map[string]any
		require.NoError(t, json.Unmarshal([]byte(entry.Metadata), &metadata))
		require.Equal(t, float64(5), metadata["video_duration_seconds"])
		require.Equal(t, "cloud-ru-usd-2026-07-15-r8", metadata["organization_price_profile_id"])
	}
}
