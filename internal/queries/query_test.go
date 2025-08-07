package queries

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	stroppy "github.com/stroppy-io/stroppy-core/pkg/proto"
)

func TestNewQuery_Success(t *testing.T) {
	descriptor := &stroppy.QueryDescriptor{
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
	}
	step := &stroppy.StepDescriptor{
		Name: "test",
		Queries: []*stroppy.StepQueryDescriptor{
			{
				Type: &stroppy.StepQueryDescriptor_Query{
					Query: descriptor,
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
	qlist, err := NewQuery(ctx, lg, gens, buildContext, descriptor)
	require.NoError(t, err)
	require.NotNil(t, qlist)
	require.Len(t, qlist.Queries, 1)
	require.Equal(t, int32(10), qlist.Queries[0].Params[0].GetInt32())
}

func TestNewQuery_Error(t *testing.T) {
	descriptor := &stroppy.QueryDescriptor{
		Name:   "q1",
		Sql:    "SELECT * FROM t WHERE id=${id}",
		Params: []*stroppy.QueryParamDescriptor{}, // нет генераторов
		Count:  1,
	}
	step := &stroppy.StepDescriptor{
		Name: "test",
		Queries: []*stroppy.StepQueryDescriptor{
			{
				Type: &stroppy.StepQueryDescriptor_Query{
					Query: descriptor,
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
	qlist, err := NewQuery(ctx, lg, gens, buildContext, descriptor)
	require.NoError(t, err)
	require.Len(t, qlist.Queries, 1)
	require.Empty(t, qlist.Queries[0].Params)
}
