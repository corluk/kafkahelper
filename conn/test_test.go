package conn

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	tcKafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

type mockRand struct{ mock.Mock }

func newMockRand() *mockRand { return &mockRand{} }
func (m *mockRand) randomInt(max int) int {
	args := m.Called(max)
	return args.Int(0)
}

func TestMock(t *testing.T) {
	var m mockRand
	m.On("randomInt", 10).Return(6)

}

func TestWithRedis(t *testing.T) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "redis:latest",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
	}
	redisC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := redisC.Terminate(ctx); err != nil {
			panic(err)
		}
	}()
}


func TestKafkaContainer(t *testing.T) {
	ctx := context.Background()

kafkaContainer, err := tcKafka.RunContainer(ctx,
	tcKafka.WithClusterID("test-cluster"),
	testcontainers.WithImage("confluentinc/confluent-local:7.5.0"),
)
if err != nil {
	panic(err)
}

// Clean up the container after
defer func() {
	if err := kafkaContainer.Terminate(ctx); err != nil {
		panic(err)
	}
}()
// }

state, err := kafkaContainer.State(ctx)
if err != nil {
	panic(err)
}

fmt.Println(kafkaContainer.ClusterID)
fmt.Println(state.Running)
}