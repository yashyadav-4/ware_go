package api

import (
	"net/http"

	"github.com/yashyadav-4/ware_go/internal/agent"
)

type AgentHandler struct {
	rebalancer *agent.Rebalancer
}

func NewAgentHandler(rebalancer *agent.Rebalancer) *AgentHandler {
	return &AgentHandler{
		rebalancer: rebalancer,
	}
}

func (h *AgentHandler) GenerateProposals(w http.ResponseWriter, r *http.Request) {
	report, err := h.rebalancer.GenerateProposals(r.Context())
	if err != nil {
		WriteError(w, http.StatusInternalServerError, "failed to generate rebalance proposals: "+err.Error())
		return
	}
	WriteJSON(w, http.StatusOK, report)
}

func (h *AgentHandler) ExecuteTransfer(w http.ResponseWriter, r *http.Request) {
	report, err := h.rebalancer.ExecuteLatestProposal(r.Context())
	if err != nil {
		WriteError(w, http.StatusConflict, err.Error())
		return
	}

	WriteJSON(w, http.StatusOK, map[string]interface{}{
		"status":   "executed",
		"message":  "latest proposal successfully executed inside ACID transaction",
		"proposal": report,
	})
}