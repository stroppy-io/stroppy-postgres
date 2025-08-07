package queries

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"

	stroppy "github.com/stroppy-io/stroppy-core/pkg/proto"
)

func newQuery(
	generators Generators,
	buildContext *stroppy.StepContext,
	descriptor *stroppy.QueryDescriptor,
) (*stroppy.DriverQuery, error) {
	paramsValues := make([]*stroppy.Value, 0)

	for _, column := range descriptor.GetParams() {
		gen, ok := generators.Get(NewGeneratorID(
			buildContext.GetStep().GetName(),
			descriptor.GetName(),
			column.GetName(),
		))
		if !ok {
			return nil, fmt.Errorf("no generator for column %s", column.GetName()) //nolint: err113
		}

		protoValue, err := gen.Next()
		if err != nil {
			return nil, fmt.Errorf(
				"failed to generate value for column %s: %w",
				column.GetName(),
				err,
			)
		}

		paramsValues = append(paramsValues, protoValue)
	}

	resSQL := descriptor.GetSql()

	for idx, param := range descriptor.GetParams() {
		// TODO: evaluate replace regex
		resSQL = strings.ReplaceAll(
			resSQL,
			fmt.Sprintf("${%s}", param.GetName()),
			fmt.Sprintf("$%d", idx+1),
		)
	}

	return &stroppy.DriverQuery{
		Name:    descriptor.GetName(),
		Request: resSQL,
		Params:  paramsValues,
	}, nil
}

func NewQuery(
	_ context.Context,
	lg *zap.Logger,
	generators Generators,
	buildContext *stroppy.StepContext,
	descriptor *stroppy.QueryDescriptor,
) (*stroppy.DriverQueriesList, error) {
	lg.Debug("build query",
		zap.String("name", descriptor.GetName()),
		zap.String("query", descriptor.GetSql()),
		zap.Any("params", descriptor.GetParams()),
	)

	queries := make([]*stroppy.DriverQuery, 0)

	for i := uint64(0); i < descriptor.GetCount(); i++ { //nolint: intrange // allow
		query, err := newQuery(generators, buildContext, descriptor)
		if err != nil {
			return nil, err
		}

		queries = append(queries, query)
	}

	return &stroppy.DriverQueriesList{
		Queries: queries,
	}, nil
}
