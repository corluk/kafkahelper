package conn

import (
	"testing"

	"github.com/stretchr/testify/mock"
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
