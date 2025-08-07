package pool

import (
	"context"
	"time"

	pgxdecimal "github.com/jackc/pgx-shopspring-decimal"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/multitracer"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	stroppy "github.com/stroppy-io/stroppy-core/pkg/proto"
	"github.com/stroppy-io/stroppy-core/pkg/protovalue"
)

const (
	LoggerName       = "pgx-pool"
	DriverLoggerName = "postgres-driver"
)

const (
	traceLogLevelKey   = "trace_log_level"
	maxConnLifetimeKey = "max_conn_lifetime"
	maxConnIdleTimeKey = "max_conn_idle_time"
	maxConnsKey        = "max_conns"
	minConnsKey        = "min_conns"
	minIdleConnsKey    = "min_idle_conns"
)

func parseConfig(
	config *stroppy.DriverConfig,
	logger *zap.Logger,
) (*pgxpool.Config, error) {
	cfgMap, err := protovalue.ValueStructToMap(config.GetDbSpecific())
	if err != nil {
		return nil, err
	}

	cfg, err := pgxpool.ParseConfig(config.GetUrl())
	if err != nil {
		return nil, err
	}

	logLevel, ok := cfgMap[traceLogLevelKey]
	if !ok {
		logLevel = "info"
	}

	lvl, err := zapcore.ParseLevel(logLevel.(string)) //nolint: errcheck,forcetypeassert // allow panic
	if err != nil {
		return nil, err
	}

	loggerTracer, err := newLoggerTracer(logger.WithOptions(
		zap.AddCallerSkip(1),
		zap.IncreaseLevel(lvl)))
	if err != nil {
		return nil, err
	}

	cfg.ConnConfig.Tracer = multitracer.New(loggerTracer)

	maxConnLifetime, ok := cfgMap[maxConnLifetimeKey]
	if ok {
		d, err := time.ParseDuration(maxConnLifetime.(string)) //nolint: errcheck,forcetypeassert // allow panic
		if err != nil {
			return nil, err
		}

		cfg.MaxConnLifetime = d
	}

	maxConnIdleTime, ok := cfgMap[maxConnIdleTimeKey]
	if ok {
		d, err := time.ParseDuration(maxConnIdleTime.(string)) //nolint: errcheck,forcetypeassert // allow panic
		if err != nil {
			return nil, err
		}

		cfg.MaxConnIdleTime = d
	}

	maxConns, ok := cfgMap[maxConnsKey]
	if ok {
		cfg.MaxConns = maxConns.(int32) //nolint: errcheck,forcetypeassert // allow panic
	}

	minConns, ok := cfgMap[minConnsKey]
	if ok {
		cfg.MinConns = minConns.(int32) //nolint: errcheck,forcetypeassert // allow panic
	}

	minIdleConns, ok := cfgMap[minIdleConnsKey]
	if ok {
		cfg.MinIdleConns = minIdleConns.(int32) //nolint: errcheck,forcetypeassert // allow panic
	}

	cfg.AfterConnect = func(_ context.Context, conn *pgx.Conn) error {
		pgxdecimal.Register(conn.TypeMap())

		return nil
	}

	return cfg, nil
}

func NewPool(
	ctx context.Context,
	config *stroppy.DriverConfig,
	logger *zap.Logger,
) (*pgxpool.Pool, error) {
	parsedConfig, err := parseConfig(config, logger)
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.NewWithConfig(ctx, parsedConfig)
	if err != nil {
		return nil, err
	}

	return pool, nil
}
