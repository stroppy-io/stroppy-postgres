package queries

import (
	"context"

	"go.uber.org/zap"

	stroppy "github.com/stroppy-io/stroppy-core/pkg/proto"
)

const (
	beginTransaction  = "BEGIN;"
	commitTransaction = "COMMIT;"
)

func startTxSQL(descriptor *stroppy.TransactionDescriptor) string {
	switch descriptor.GetIsolationLevel() {
	case stroppy.TxIsolationLevel_TX_ISOLATION_LEVEL_UNSPECIFIED:
		return beginTransaction
	case stroppy.TxIsolationLevel_TX_ISOLATION_LEVEL_READ_UNCOMMITTED:
		return "BEGIN ISOLATION LEVEL READ UNCOMMITTED;"
	case stroppy.TxIsolationLevel_TX_ISOLATION_LEVEL_READ_COMMITTED:
		return "BEGIN ISOLATION LEVEL READ COMMITTED;"
	case stroppy.TxIsolationLevel_TX_ISOLATION_LEVEL_REPEATABLE_READ:
		return "BEGIN ISOLATION LEVEL REPEATABLE READ;"
	case stroppy.TxIsolationLevel_TX_ISOLATION_LEVEL_SERIALIZABLE:
		return "BEGIN ISOLATION LEVEL SERIALIZABLE;"
	default:
		return beginTransaction
	}
}

func NewTransaction(
	ctx context.Context,
	lg *zap.Logger,
	generators Generators,
	buildContext *stroppy.StepContext,
	descriptor *stroppy.TransactionDescriptor,
) (*stroppy.DriverQueriesList, error) {
	lg.Debug("build transaction",
		zap.String("name", descriptor.GetName()))

	var queries []*stroppy.DriverQuery

	queries = append(queries, &stroppy.DriverQuery{
		Name:    "begin_transaction",
		Request: startTxSQL(descriptor),
	})

	for _, query := range descriptor.GetQueries() {
		q, err := NewQuery(ctx, lg, generators, buildContext, query)
		if err != nil {
			return nil, err
		}

		queries = append(queries, q.GetQueries()...)
	}

	queries = append(queries, &stroppy.DriverQuery{
		Name:    "commit_transaction",
		Request: commitTransaction,
	})

	return &stroppy.DriverQueriesList{
		Queries: queries,
	}, nil
}
