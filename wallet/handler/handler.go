package handler

import (
	"context"
	"log/slog"

	walletv1 "github.com/aien-platform/aien/proto/wallet/v1"
	"github.com/aien-platform/aien/wallet/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Handler implements the gRPC WalletServiceServer interface.
type Handler struct {
	walletv1.UnimplementedWalletServiceServer
	store  *store.PostgresStore
	logger *slog.Logger
}

// New instantiates a new gRPC Wallet handler.
func New(store *store.PostgresStore, logger *slog.Logger) *Handler {
	return &Handler{
		store:  store,
		logger: logger,
	}
}

// GetBalance returns the balance of a wallet account.
func (h *Handler) GetBalance(ctx context.Context, req *walletv1.GetBalanceRequest) (*walletv1.GetBalanceResponse, error) {
	if req.AccountId == "" {
		return nil, status.Error(codes.InvalidArgument, "account_id cannot be empty")
	}

	balance, err := h.store.GetBalance(ctx, req.AccountId)
	if err != nil {
		h.logger.Warn("Failed to get balance", "account_id", req.AccountId, "error", err)
		return nil, status.Error(codes.NotFound, err.Error())
	}

	return &walletv1.GetBalanceResponse{
		AccountId: req.AccountId,
		Balance:   balance,
	}, nil
}

// Credit adds funds to an account.
func (h *Handler) Credit(ctx context.Context, req *walletv1.CreditRequest) (*walletv1.CreditResponse, error) {
	if req.AccountId == "" {
		return nil, status.Error(codes.InvalidArgument, "account_id cannot be empty")
	}
	if req.Amount <= 0 {
		return nil, status.Error(codes.InvalidArgument, "amount must be greater than zero")
	}
	if req.ReferenceId == "" {
		return nil, status.Error(codes.InvalidArgument, "reference_id cannot be empty")
	}

	newBalance, err := h.store.Credit(ctx, req.AccountId, req.Amount, req.ReferenceId)
	if err != nil {
		h.logger.Error("Failed to credit account", "account_id", req.AccountId, "amount", req.Amount, "error", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	h.logger.Info("Credited account", "account_id", req.AccountId, "amount", req.Amount, "new_balance", newBalance)
	return &walletv1.CreditResponse{
		AccountId:  req.AccountId,
		NewBalance: newBalance,
	}, nil
}

// Debit subtracts funds from an account.
func (h *Handler) Debit(ctx context.Context, req *walletv1.DebitRequest) (*walletv1.DebitResponse, error) {
	if req.AccountId == "" {
		return nil, status.Error(codes.InvalidArgument, "account_id cannot be empty")
	}
	if req.Amount <= 0 {
		return nil, status.Error(codes.InvalidArgument, "amount must be greater than zero")
	}
	if req.ReferenceId == "" {
		return nil, status.Error(codes.InvalidArgument, "reference_id cannot be empty")
	}

	newBalance, err := h.store.Debit(ctx, req.AccountId, req.Amount, req.ReferenceId)
	if err != nil {
		h.logger.Error("Failed to debit account", "account_id", req.AccountId, "amount", req.Amount, "error", err)
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	h.logger.Info("Debited account", "account_id", req.AccountId, "amount", req.Amount, "new_balance", newBalance)
	return &walletv1.DebitResponse{
		AccountId:  req.AccountId,
		NewBalance: newBalance,
	}, nil
}

// Transfer atomically transfers funds between accounts.
func (h *Handler) Transfer(ctx context.Context, req *walletv1.TransferRequest) (*walletv1.TransferResponse, error) {
	if req.FromAccountId == "" || req.ToAccountId == "" {
		return nil, status.Error(codes.InvalidArgument, "from_account_id and to_account_id cannot be empty")
	}
	if req.Amount <= 0 {
		return nil, status.Error(codes.InvalidArgument, "amount must be greater than zero")
	}
	if req.ReferenceId == "" {
		return nil, status.Error(codes.InvalidArgument, "reference_id cannot be empty")
	}

	fromNewBal, toNewBal, err := h.store.Transfer(ctx, req.FromAccountId, req.ToAccountId, req.Amount, req.ReferenceId)
	if err != nil {
		h.logger.Error("Failed transfer request", "from", req.FromAccountId, "to", req.ToAccountId, "amount", req.Amount, "error", err)
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	h.logger.Info("Transfer execution succeeded", 
		"from", req.FromAccountId, 
		"to", req.ToAccountId, 
		"amount", req.Amount, 
		"from_new_balance", fromNewBal, 
		"to_new_balance", toNewBal,
	)

	return &walletv1.TransferResponse{
		FromAccountId:      req.FromAccountId,
		ToAccountId:        req.ToAccountId,
		FromNewBalance:     fromNewBal,
		ToNewBalance:       toNewBal,
	}, nil
}
