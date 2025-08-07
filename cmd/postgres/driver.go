package main

import (
	"context"

	"github.com/jackc/pgx/v5/pgconn"
	"go.uber.org/zap"

	"github.com/stroppy-io/stroppy-core/pkg/logger"
	"github.com/stroppy-io/stroppy-core/pkg/plugins/driver"
	stroppy "github.com/stroppy-io/stroppy-core/pkg/proto"

	"github.com/stroppy-io/stroppy-postgres/internal/pool"
	"github.com/stroppy-io/stroppy-postgres/internal/queries"
)

type Connection interface {
	// Exec executes the given SQL statement with the provided arguments in the context of the Executor.
	//
	// Parameters:
	// - ctx: The context.Context object.
	// - sql: The SQL statement to execute.
	// - arguments: The arguments to be passed to the SQL statement.
	//
	// Returns:
	// - pgconn.CommandTag: The command tag returned by the execution.
	// - error: An error if the execution fails.
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Close()
}

type QueryBuilder interface {
	Build(
		ctx context.Context,
		logger *zap.Logger,
		buildQueriesContext *stroppy.BuildQueriesContext,
	) (*stroppy.DriverQueriesList, error)
	ValueToPgxValue(value *stroppy.Value) (any, error)
}

type Driver struct {
	logger   *zap.Logger
	connPool Connection
	builder  QueryBuilder
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
		runContext.GetConfig().GetDriver(),
		d.logger.Named(pool.LoggerName),
	)
	if err != nil {
		return err
	}

	d.connPool = connPool

	d.builder, err = queries.NewQueryBuilder(runContext)
	if err != nil {
		return err
	}

	return nil
}

func (d *Driver) BuildQueries(
	ctx context.Context,
	buildQueriesContext *stroppy.BuildQueriesContext,
) (*stroppy.DriverQueriesList, error) {
	return d.builder.Build(ctx, d.logger, buildQueriesContext)
}

func (d *Driver) RunQuery(ctx context.Context, query *stroppy.DriverQuery) error {
	d.logger.Debug(
		"run query",
		zap.String("name", query.GetName()),
		zap.String("sql", query.GetRequest()),
		zap.Any("args", query.GetParams()),
	)

	values := make([]any, len(query.GetParams()))

	for i, v := range query.GetParams() {
		val, err := d.builder.ValueToPgxValue(v)
		if err != nil {
			return err
		}

		values[i] = val
	}

	_, err := d.connPool.Exec(ctx, query.GetRequest(), values...)

	return err
}

func (d *Driver) Teardown(_ context.Context) error {
	d.connPool.Close()

	return nil
}
