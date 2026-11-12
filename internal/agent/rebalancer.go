package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/generative-ai-go/genai"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yashyadav-4/ware_go/internal/repository/db"
	"google.golang.org/api/option"
)

type TransferProposal struct {
	SourceWarehouseID   string `json:"source_warehouse_id"`
	SourceWarehouseCode string `json:"source_warehouse_code"`
	DestWarehouseID     string `json:"dest_warehouse_id"`
	DestWarehouseCode   string `json:"dest_warehouse_code"`
	SKU                 string `json:"sku"`
	Quantity            int32  `json:"quantity"`
	Reasoning           string `json:"reasoning"`
	EstimatedImpact     string `json:"estimated_impact"`
}

type RebalanceReport struct {
	ID        string             `json:"id,omitempty"`
	Status    string             `json:"status,omitempty"`
	Timestamp time.Time          `json:"timestamp"`
	Analysis  string             `json:"analysis"`
	Proposals []TransferProposal `json:"proposals"`
}

type Rebalancer struct {
	pool      *pgxpool.Pool
	queries   *db.Queries
	apiKey    string
	modelName string
}

func NewRebalancer(pool *pgxpool.Pool, queries *db.Queries, apiKey string, modelName string) *Rebalancer {
	if strings.TrimSpace(modelName) == "" {
		modelName = "gemini-3.8-flash"
	}
	return &Rebalancer{
		pool:      pool,
		queries:   queries,
		apiKey:    apiKey,
		modelName: modelName,
	}
}

func (r *Rebalancer) GenerateProposals(ctx context.Context) (*RebalanceReport, error) {
	if strings.TrimSpace(r.apiKey) == "" {
		return nil, errors.New("GEMINI_API_KEY is not configured")
	}

	items, err := r.queries.ListAllInventory(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch inventory for rebalancer: %w", err)
	}

	if len(items) == 0 {
		return &RebalanceReport{
			Timestamp: time.Now(),
			Analysis:  "No active inventory records found across warehouses.",
			Proposals: []TransferProposal{},
		}, nil
	}

	inventoryJSON, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to serialize inventory snapshot: %w", err)
	}

	client, err := genai.NewClient(ctx, option.WithAPIKey(r.apiKey))
	if err != nil {
		return nil, fmt.Errorf("failed to initialize gemini client: %w", err)
	}
	defer client.Close()

	model := client.GenerativeModel(r.modelName)
	model.ResponseMIMEType = "application/json"

	prompt := fmt.Sprintf(`
You are the Autonomous Supply Chain Optimizer for Ware_Go, a high-concurrency multi-warehouse engine.
Analyze the following warehouse inventory snapshot, including geographic coordinates and stock levels.
Identify inventory imbalances where one warehouse has severe stock deficit while another has surplus.

Rules:
1. Only propose transfers from a warehouse with surplus (> 50 available) to a warehouse with low stock (< 20 available).
2. Transfer quantity must not exceed 50%% of the source warehouse's available stock.
3. Every proposal must include concrete, data-backed reasoning.
4. Output STRICTLY as a JSON object matching this schema:
{
  "analysis": "High-level summary of network inventory health",
  "proposals": [
    {
      "source_warehouse_id": "UUID",
      "source_warehouse_code": "CODE",
      "dest_warehouse_id": "UUID",
      "dest_warehouse_code": "CODE",
      "sku": "SKU",
      "quantity": 100,
      "reasoning": "Reason for rebalancing",
      "estimated_impact": "Latency / stockout risk mitigation"
    }
  ]
}

Current Warehouse Inventory Snapshot:
%s
`, string(inventoryJSON))

	resp, err := model.GenerateContent(ctx, genai.Text(prompt))
	if err != nil {
		return nil, fmt.Errorf("gemini api call failed: %w", err)
	}

	if len(resp.Candidates) == 0 || resp.Candidates[0].Content == nil || len(resp.Candidates[0].Content.Parts) == 0 {
		return nil, errors.New("empty response received from gemini")
	}

	part, ok := resp.Candidates[0].Content.Parts[0].(genai.Text)
	if !ok {
		return nil, errors.New("unexpected content format received from gemini")
	}

	var report RebalanceReport
	if err := json.Unmarshal([]byte(part), &report); err != nil {
		return nil, fmt.Errorf("failed to parse gemini json output: %w", err)
	}

	proposalsRaw, err := json.Marshal(report.Proposals)
	if err != nil {
		return nil, fmt.Errorf("failed to encode proposals json: %w", err)
	}

	stored, err := r.queries.InsertRebalanceProposal(ctx, db.InsertRebalanceProposalParams{
		Analysis:      report.Analysis,
		ProposalsJson: proposalsRaw,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to persist proposal to database: %w", err)
	}

	report.ID = fmt.Sprintf("%x-%x-%x-%x-%x", stored.ID.Bytes[0:4], stored.ID.Bytes[4:6], stored.ID.Bytes[6:8], stored.ID.Bytes[8:10], stored.ID.Bytes[10:16])
	report.Status = stored.Status
	report.Timestamp = stored.CreatedAt.Time

	return &report, nil
}

func (r *Rebalancer) ExecuteLatestProposal(ctx context.Context) (*RebalanceReport, error) {
	latest, err := r.queries.GetLatestRebalanceProposal(ctx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("no rebalance proposal found; please generate proposals first")
		}
		return nil, fmt.Errorf("failed to query latest proposal: %w", err)
	}

	if latest.Status == "EXECUTED" {
		return nil, errors.New("the latest rebalance proposal has already been executed")
	}

	var proposals []TransferProposal
	if err := json.Unmarshal(latest.ProposalsJson, &proposals); err != nil {
		return nil, fmt.Errorf("corrupted proposals json in database: %w", err)
	}

	if len(proposals) == 0 {
		return nil, errors.New("latest proposal contains 0 transfer items")
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	qtx := r.queries.WithTx(tx)

	for _, p := range proposals {
		if p.Quantity <= 0 {
			continue
		}

		var srcUUID, dstUUID pgtype.UUID
		if err := srcUUID.Scan(p.SourceWarehouseID); err != nil || !srcUUID.Valid {
			return nil, fmt.Errorf("invalid source_warehouse_id '%s': %w", p.SourceWarehouseID, err)
		}
		if err := dstUUID.Scan(p.DestWarehouseID); err != nil || !dstUUID.Valid {
			return nil, fmt.Errorf("invalid dest_warehouse_id '%s': %w", p.DestWarehouseID, err)
		}

		_, err = qtx.DeductInventoryStock(ctx, db.DeductInventoryStockParams{
			WarehouseID:  srcUUID,
			Sku:          p.SKU,
			AvailableQty: p.Quantity,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to deduct %d of SKU '%s' from source warehouse: %w", p.Quantity, p.SKU, err)
		}

		_, err = qtx.UpsertRestockInventory(ctx, db.UpsertRestockInventoryParams{
			WarehouseID:  dstUUID,
			Sku:          p.SKU,
			AvailableQty: p.Quantity,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to restock destination warehouse: %w", err)
		}

		_, err = qtx.InsertAuditlog(ctx, db.InsertAuditlogParams{
			WarehouseID:    srcUUID,
			Sku:            p.SKU,
			OrderID:        pgtype.UUID{Valid: false},
			EventType:      "TRANSFER_OUT",
			DeltaAvailable: -p.Quantity,
			DeltaReserved:  0,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to write audit log for transfer out: %w", err)
		}

		_, err = qtx.InsertAuditlog(ctx, db.InsertAuditlogParams{
			WarehouseID:    dstUUID,
			Sku:            p.SKU,
			OrderID:        pgtype.UUID{Valid: false},
			EventType:      "TRANSFER_IN",
			DeltaAvailable: p.Quantity,
			DeltaReserved:  0,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to write audit log for transfer in: %w", err)
		}
	}

	_, err = qtx.MarkRebalanceProposalExecuted(ctx, latest.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to mark proposal as executed: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transfer transaction: %w", err)
	}

	return &RebalanceReport{
		ID:        fmt.Sprintf("%x-%x-%x-%x-%x", latest.ID.Bytes[0:4], latest.ID.Bytes[4:6], latest.ID.Bytes[6:8], latest.ID.Bytes[8:10], latest.ID.Bytes[10:16]),
		Status:    "EXECUTED",
		Timestamp: time.Now(),
		Analysis:  latest.Analysis,
		Proposals: proposals,
	}, nil
}