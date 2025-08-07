package queries

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	stroppy "github.com/stroppy-io/stroppy-core/pkg/proto"
)

func TestNewCreateTable_Success(t *testing.T) {
	descriptor := &stroppy.TableDescriptor{
		Name:    "t1",
		Columns: []*stroppy.ColumnDescriptor{{Name: "id", SqlType: "INT", PrimaryKey: true}},
	}
	ctx := context.Background()
	lg := zap.NewNop()
	qlist, err := NewCreateTable(ctx, lg, 42, descriptor)
	require.NoError(t, err)
	require.NotNil(t, qlist)
	require.NotEmpty(t, qlist.Queries)
}

func TestNewCreateTable_Error(t *testing.T) {
	descriptor := &stroppy.TableDescriptor{
		Name:    "t1",
		Columns: nil, // нет колонок
	}
	ctx := context.Background()
	lg := zap.NewNop()
	table, err := NewCreateTable(ctx, lg, 42, descriptor)
	require.NoError(t, err)
	require.NotNil(t, table)
	require.NotEmpty(t, table.Queries)
	require.Empty(t, table.Queries[0].Params)
}
