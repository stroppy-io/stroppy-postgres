package main

import (
	"context"

	trmpgx "github.com/avito-tech/go-transaction-manager/drivers/pgxv5/v2"
	"github.com/avito-tech/go-transaction-manager/trm/v2/manager"
	"go.uber.org/zap"

	"github.com/stroppy-io/stroppy-core/pkg/logger"
	"github.com/stroppy-io/stroppy-core/pkg/plugins/driver"
	stroppy "github.com/stroppy-io/stroppy-core/pkg/proto"
	"github.com/stroppy-io/stroppy-core/pkg/utils/errchan"

	"github.com/stroppy-io/stroppy-postgres/internal/pool"
	"github.com/stroppy-io/stroppy-postgres/internal/queries"
)

type QueryBuilder interface {
	Build(
		ctx context.Context,
		logger *zap.Logger,
		buildQueriesContext *stroppy.UnitBuildContext,
	) (*stroppy.DriverTransactionList, error)
	BuildStream(
		ctx context.Context,
		logger *zap.Logger,
		buildQueriesContext *stroppy.UnitBuildContext,
		channel errchan.Chan[stroppy.DriverTransaction],
	)
	ValueToPgxValue(value *stroppy.Value) (any, error)
}

type Driver struct {
	logger  *zap.Logger
	pgxPool interface {
		Executor
		Close()
	}
	txManager  *manager.Manager
	txExecutor *TxExecutor
	builder    QueryBuilder
}

func NewDriver() driver.Plugin { //nolint: ireturn // allow
	return &Driver{
		logger: logger.NewFromEnv().
			Named(pool.DriverLoggerName).
			WithOptions(zap.AddCallerSkip(1)),
	}
}

func (d *Driver) Initialize(ctx context.Context, runContext *stroppy.StepContext) error {
	connPool, err := pool.NewPool(
		ctx,
		runContext.GetGlobalConfig().GetRun().GetDriver(),
		d.logger.Named(pool.LoggerName),
	)
	if err != nil {
		return err
	}

	d.pgxPool = connPool

	d.builder, err = queries.NewQueryBuilder(runContext)
	if err != nil {
		return err
	}

	d.txManager = manager.Must(trmpgx.NewDefaultFactory(connPool))
	d.txExecutor = NewTxExecutor(connPool)

	return nil
}

func (d *Driver) BuildTransactionsFromUnit(
	ctx context.Context,
	buildUnitContext *stroppy.UnitBuildContext,
) (*stroppy.DriverTransactionList, error) {
	return d.builder.Build(ctx, d.logger, buildUnitContext)
}

func (d *Driver) BuildTransactionsFromUnitStream(
	ctx context.Context,
	buildUnitContext *stroppy.UnitBuildContext,
) (errchan.Chan[stroppy.DriverTransaction], error) {
	channel := make(errchan.Chan[stroppy.DriverTransaction])
	go func() {
		d.builder.BuildStream(ctx, d.logger, buildUnitContext, channel)
	}()

	return channel, nil
}

func (d *Driver) RunTransaction(
	ctx context.Context,
	transaction *stroppy.DriverTransaction,
) error {
	if transaction.GetIsolationLevel() == stroppy.TxIsolationLevel_TX_ISOLATION_LEVEL_UNSPECIFIED {
		return d.runTransactionInternal(ctx, transaction, d.pgxPool)
	}

	return d.txManager.DoWithSettings(
		ctx,
		NewStroppyIsolationSettings(transaction),
		func(ctx context.Context) error {
			return d.runTransactionInternal(ctx, transaction, d.txExecutor)
		})
}

func (d *Driver) runTransactionInternal(
	ctx context.Context,
	transaction *stroppy.DriverTransaction,
	executor Executor,
) error {
	for _, query := range transaction.GetQueries() {
		values := make([]any, len(query.GetParams()))

		for i, v := range query.GetParams() {
			val, err := d.builder.ValueToPgxValue(v)
			if err != nil {
				return err
			}

			values[i] = val
		}

		_, err := executor.Exec(ctx, query.GetRequest(), values...)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *Driver) Teardown(_ context.Context) error {
	d.pgxPool.Close()

	return nil
}
