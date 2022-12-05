package runtime

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// To support middleware state snapshot, we need a format store multiple bytes without extra
// memory allocation and keep compatibility
// [length1(uint64)] [id1(uint64)] [crc1(uint64)] [bytes1([]byte)] [length2(uint64)] [id2(uint64)] [crc2(uint64)] [bytes2([]byte)] [bytes([]byte)]
// length xor id shoud always equal crc
// the last do not have length and crc to keep compatibility

var ErrSnapshotCRCCheckFail error = errors.New("ErrSnapshotCRCCheckFail")
var ErrSnapshotIDNotMatch error = errors.New("ErrSnapshotIDNotMatch")

// when no element degrade to nil
// when one element defrade to that element
// otherwise keep itself
type MemoryMultiSnapshot struct {
	Snapshots map[uint64]any
}

func NewMemoryMultiSnapshot(state any) *MemoryMultiSnapshot {
	switch s := state.(type) {
	case nil:
		return &MemoryMultiSnapshot{Snapshots: make(map[uint64]any, 0)}
	case *MemoryMultiSnapshot:
		return s
	default:
		it := &MemoryMultiSnapshot{Snapshots: make(map[uint64]any, 0)}
		it.Push(0, state)
		return it
	}
}

func (it *MemoryMultiSnapshot) Push(id uint64, data any) error {
	if _, ok := it.Snapshots[id]; ok {
		return fmt.Errorf("AlreadyExists")
	}
	it.Snapshots[id] = data
	return nil
}

func (it *MemoryMultiSnapshot) Pop(id uint64) (any, error) {
	s, ok := it.Snapshots[id]
	if !ok {
		return nil, fmt.Errorf("NotFound")
	}
	delete(it.Snapshots, id)
	return s, nil
}

func (it *MemoryMultiSnapshot) Next() any {
	if len(it.Snapshots) == 0 {
		return nil
	} else if len(it.Snapshots) == 1 {
		for _, s := range it.Snapshots {
			return s
		}
	}
	return it
}

type MultiSnapshotWriter struct {
	w io.Writer
}

func MultiSnapshotWriteItem(w io.Writer, id uint64, item []byte) error {
	length := uint64(len(item))
	crc := length ^ id
	if err := binary.Write(w, binary.LittleEndian, length); err != nil {
		return fmt.Errorf("binary.Write length err: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, id); err != nil {
		return fmt.Errorf("binary.Write id err: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, crc); err != nil {
		return fmt.Errorf("binary.Write crc err: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, item); err != nil {
		return fmt.Errorf("binary.Write item err: %w", err)
	}
	return nil
}

func MultiSnapshotPeekInfo(r *bufio.Reader) (uint64, uint64, error) {
	header, err := r.Peek(8*3)
	if err != nil {
		return 0, 0, fmt.Errorf("reader.Peek %v err: %w", err, ErrSnapshotCRCCheckFail)
	}
	length := binary.LittleEndian.Uint64(header[0:])
	id := binary.LittleEndian.Uint64(header[8:])
	crc := binary.LittleEndian.Uint64(header[16:])
	if length^crc != id {
		return 0, 0, fmt.Errorf("%w: length: %d, crc: %d", ErrSnapshotCRCCheckFail, length, crc)
	}
	return length, id, nil
}

func MultiSnapshotReadItem(r *bufio.Reader, id uint64) ([]byte, error) {
	length, sid, err := MultiSnapshotPeekInfo(r)
	if err != nil {
		return nil, fmt.Errorf("MultiSnapshotPeekInfo err: %w", err)
	}
	if id != sid {
		return nil, fmt.Errorf("%w: %d != %d", ErrSnapshotIDNotMatch, id, sid)
	}
	r.Discard(8*3)
	buffer := make([]byte, int(length))
	n, err := io.ReadFull(r, buffer)
	if err != nil {
		return nil, fmt.Errorf("io.ReadFull err: %w", err)
	}
	if n != int(length) {
		return nil, fmt.Errorf("data corruped")
	}
	return buffer, nil
}
