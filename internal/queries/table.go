package queries

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"

	stroppy "github.com/stroppy-io/stroppy-core/pkg/proto"
)

func newIndex(
	tableName string,
	index *stroppy.IndexDescriptor,
) (*stroppy.DriverQuery, error) { //nolint: unparam // maybe later
	return &stroppy.DriverQuery{
		Name: "create_index_" + index.GetName(),
		Request: "CREATE INDEX IF NOT EXISTS " +
			index.GetName() + " ON " +
			tableName + " (" + strings.Join(index.GetColumns(), ", ") + ");",
	}, nil
}

func newCreateTable(
	tableName string,
	columns []*stroppy.ColumnDescriptor,
) (*stroppy.DriverQuery, error) { //nolint: unparam // maybe later
	columnsStr := make([]string, len(columns))

	for i, column := range columns {
		constants := make([]string, 0)

		if column.GetPrimaryKey() {
			constants = append(constants, "PRIMARY KEY")
		}

		if !column.GetNullable() {
			constants = append(constants, "NOT NULL")
		}

		if column.GetUnique() {
			constants = append(constants, "UNIQUE")
		}

		if column.GetConstraint() != "" {
			constants = []string{column.GetConstraint()}
		}

		columnsStr[i] = fmt.Sprintf(
			"%s %s %s",
			column.GetName(),
			column.GetSqlType(),
			strings.Join(constants, " "),
		)
	}

	return &stroppy.DriverQuery{
		Name: "create_table_" + tableName,
		Request: "CREATE TABLE IF NOT EXISTS " +
			tableName + " (" + strings.Join(columnsStr, ", ") + ");",
	}, nil
}

//goland:noinspection t
func NewCreateTable(
	_ context.Context,
	lg *zap.Logger,
	_ uint64,
	descriptor *stroppy.TableDescriptor,
) (*stroppy.DriverQueriesList, error) {
	lg.Debug("build table",
		zap.String("name", descriptor.GetName()),
		zap.Any("columns", descriptor.GetColumns()))

	queries := make([]*stroppy.DriverQuery, 0)

	createTableQ, err := newCreateTable(descriptor.GetName(), descriptor.GetColumns())
	if err != nil {
		return nil, err
	}

	lg.Debug("create table query",
		zap.String("name", descriptor.GetName()),
		zap.Any("columns", descriptor.GetColumns()),
		zap.Any("query", createTableQ),
		zap.Error(err),
	)

	queries = append(queries, createTableQ)

	for _, index := range descriptor.GetTableIndexes() {
		indexQ, err := newIndex(descriptor.GetName(), index)
		if err != nil {
			return nil, err
		}

		lg.Debug("create index query",
			zap.String("name", descriptor.GetName()),
			zap.Any("index", index),
			zap.Any("query", indexQ),
			zap.Error(err),
		)

		queries = append(queries, indexQ)
	}

	return &stroppy.DriverQueriesList{
		Queries: queries,
	}, nil
}
