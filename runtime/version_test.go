package runtime

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestVersion(t *testing.T) {
	cver := "code1"
	vm := NewVersionManager(NewMemoryStroage(), cver)

	assert.True(t, vm.CodeVersion(0) == cver)
	assert.True(t, vm.DataVersion(0) == "")
	vm.SetDataVersion(0, vm.CodeVersion(0))
	assert.True(t, vm.DataVersion(0) == cver)
	assert.True(t, vm.Dirty(0))
	vm.Rollback(0)
	assert.True(t, vm.DataVersion(0) == "")
	vm.SetDataVersion(0, vm.CodeVersion(0))
	vm.Commit(0)
	assert.True(t, vm.DataVersion(0) == cver)
}
