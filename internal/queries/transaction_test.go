package queries

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	stroppy "github.com/stroppy-io/stroppy-core/pkg/proto"
)

func TestNewTransaction_Success(t *testing.T) {
	descriptor := &stroppy.TransactionDescriptor{
		Name: "t1",
		Queries: []*stroppy.QueryDescriptor{
			{
				Name: "q1",
				Sql:  "SELECT * FROM t WHERE id=${id}",
				Params: []*stroppy.QueryParamDescriptor{{Name: "id", GenerationRule: &stroppy.Generation_Rule{
					Type: &stroppy.Generation_Rule_Int32Rules{
						Int32Rules: &stroppy.Generation_Rules_Int32Rule{
							Constant: proto.Int32(10),
						},
					},
				}}},
				Count: 1,
			},
		},
	}
	step := &stroppy.StepDescriptor{
		Name: "test",
		Queries: []*stroppy.StepQueryDescriptor{
			{
				Type: &stroppy.StepQueryDescriptor_Transaction{
					Transaction: descriptor,
				},
			},
		},
	}
	buildContext := &stroppy.StepContext{
		Config: &stroppy.RunConfig{
			Seed: 42,
		},
		Step: step,
		Benchmark: &stroppy.BenchmarkDescriptor{
			Steps: []*stroppy.StepDescriptor{
				step,
			},
		},
	}
	ctx := context.Background()
	lg := zap.NewNop()
	gens, err := CollectStepGenerators(buildContext)
	require.NoError(t, err)
	qlist, err := NewTransaction(ctx, lg, gens, buildContext, descriptor)
	require.NoError(t, err)
	require.NotNil(t, qlist)
	require.Len(t, qlist.Queries, 3)
	require.Equal(t, beginTransaction, qlist.Queries[0].Request)
	require.Equal(t, "SELECT * FROM t WHERE id=$1", qlist.Queries[1].Request)
	require.Equal(t, commitTransaction, qlist.Queries[2].Request)
	require.Equal(t, int32(10), qlist.Queries[1].Params[0].GetInt32())
}

func TestNewTransaction_Isolation(t *testing.T) {
	descriptor := &stroppy.TransactionDescriptor{
		Name:           "t1",
		IsolationLevel: stroppy.TxIsolationLevel_TX_ISOLATION_LEVEL_READ_UNCOMMITTED,
		Queries: []*stroppy.QueryDescriptor{
			{
				Name: "q1",
				Sql:  "SELECT * FROM t WHERE id=${id}",
				Params: []*stroppy.QueryParamDescriptor{{Name: "id", GenerationRule: &stroppy.Generation_Rule{
					Type: &stroppy.Generation_Rule_Int32Rules{
						Int32Rules: &stroppy.Generation_Rules_Int32Rule{
							Constant: proto.Int32(10),
						},
					},
				}}},
				Count: 1,
			},
		},
	}
	step := &stroppy.StepDescriptor{
		Name: "test",
		Queries: []*stroppy.StepQueryDescriptor{
			{
				Type: &stroppy.StepQueryDescriptor_Transaction{
					Transaction: descriptor,
				},
			},
		},
	}
	buildContext := &stroppy.StepContext{
		Config: &stroppy.RunConfig{
			Seed: 42,
		},
		Step: step,
		Benchmark: &stroppy.BenchmarkDescriptor{
			Steps: []*stroppy.StepDescriptor{
				step,
			},
		},
	}
	ctx := context.Background()
	lg := zap.NewNop()
	gens, err := CollectStepGenerators(buildContext)
	require.NoError(t, err)
	qlist, err := NewTransaction(ctx, lg, gens, buildContext, descriptor)
	require.NoError(t, err)
	require.NotNil(t, qlist)
	require.Len(t, qlist.Queries, 3)
	require.Equal(t, "BEGIN ISOLATION LEVEL READ UNCOMMITTED;", qlist.Queries[0].Request)
	require.Equal(t, "SELECT * FROM t WHERE id=$1", qlist.Queries[1].Request)
	require.Equal(t, "COMMIT;", qlist.Queries[2].Request)
	require.Equal(t, int32(10), qlist.Queries[1].Params[0].GetInt32())
}
