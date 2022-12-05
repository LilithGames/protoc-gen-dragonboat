package runtime

import (
	"fmt"
	"math"
	"strconv"
	"bytes"
	"bufio"
	"testing"
	"io"
	"errors"

	"github.com/stretchr/testify/assert"
)

func TestCRC(t *testing.T) {
	fmt.Printf("%s\n", strconv.FormatUint(math.MaxUint64, 16))
	var a uint64 = 12345678910
	b := a ^ math.MaxUint64
	println(a ^ b == math.MaxUint64)
}

func TestSnapshotReadWrite(t *testing.T) {
	var snapshotID uint64 = 123456789
	var buffer bytes.Buffer
	w := bufio.NewWriter(&buffer)
	MultiSnapshotWriteItem(w, snapshotID, []byte{0x02, 0x03})
	w.Write([]byte{0x00, 0x01})
	w.Flush()
	r := bufio.NewReader(&buffer)

	_, err := MultiSnapshotReadItem(r, uint64(908293487))
	assert.True(t, errors.Is(err, ErrSnapshotIDNotMatch))

	data1, err := MultiSnapshotReadItem(r, snapshotID)
	assert.Nil(t, err)
	assert.Equal(t, data1, []byte{0x02, 0x03})

	_, err = MultiSnapshotReadItem(r, uint64(908293487))
	assert.True(t, errors.Is(err, ErrSnapshotCRCCheckFail))

	data2, err := io.ReadAll(r)
	assert.Nil(t, err)
	assert.Equal(t, data2, []byte{0x00, 0x01})
}

func TestMemoryMultiSnapshot(t *testing.T) {
	s1 := NewMemoryMultiSnapshot(nil)
	assert.Nil(t, s1.Next())
	s2 := NewMemoryMultiSnapshot(3)
	assert.Equal(t, 3, s2.Next())
	s2.Push(111, 4)
	assert.True(t, s2.Next().(*MemoryMultiSnapshot) != nil)
	i, err := s2.Pop(111)
	assert.Nil(t, err)
	assert.Equal(t, 4, i)
	_, err2 := s2.Pop(222)
	assert.NotNil(t, err2)
	err3 := s2.Push(0, 999)
	assert.NotNil(t, err3)
	assert.Equal(t, 3, s2.Next())
}
