package rocksstore

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"

	"github.com/google/btree"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
)

type mvccValueType int

const (
	typePut mvccValueType = iota
	typeDelete
	typeRollback
	typeLock
)

type mvccValue struct {
	valueType mvccValueType
	startTS   uint64
	commitTS  uint64
	value     []byte
}

type mvccLock struct {
	startTS     uint64
	primary     []byte
	value       []byte
	op          kvrpcpb.Op
	ttl         uint64
	forUpdateTS uint64
	txnSize     uint64
	minCommitTS uint64
}

type mvccEntry struct {
	key    MvccKey
	values []mvccValue
	lock   *mvccLock
}

// MarshalBinary implements encoding.BinaryMarshaler interface.
func (l *mvccLock) MarshalBinary() ([]byte, error) {
	var (
		mh  marshalHelper
		buf bytes.Buffer
	)
	mh.WriteNumber(&buf, l.startTS)
	mh.WriteSlice(&buf, l.primary)
	mh.WriteSlice(&buf, l.value)
	mh.WriteNumber(&buf, l.op)
	mh.WriteNumber(&buf, l.ttl)
	mh.WriteNumber(&buf, l.forUpdateTS)
	mh.WriteNumber(&buf, l.txnSize)
	mh.WriteNumber(&buf, l.minCommitTS)
	return buf.Bytes(), errors.Trace(mh.err)
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler interface.
func (l *mvccLock) UnmarshalBinary(data []byte) error {
	var mh marshalHelper
	buf := bytes.NewBuffer(data)
	mh.ReadNumber(buf, &l.startTS)
	mh.ReadSlice(buf, &l.primary)
	mh.ReadSlice(buf, &l.value)
	mh.ReadNumber(buf, &l.op)
	mh.ReadNumber(buf, &l.ttl)
	mh.ReadNumber(buf, &l.forUpdateTS)
	mh.ReadNumber(buf, &l.txnSize)
	mh.ReadNumber(buf, &l.minCommitTS)
	return errors.Trace(mh.err)
}

// MarshalBinary implements encoding.BinaryMarshaler interface.
func (v mvccValue) MarshalBinary() ([]byte, error) {
	var (
		mh  marshalHelper
		buf bytes.Buffer
	)
	mh.WriteNumber(&buf, int64(v.valueType))
	mh.WriteNumber(&buf, v.startTS)
	mh.WriteNumber(&buf, v.commitTS)
	mh.WriteSlice(&buf, v.value)
	return buf.Bytes(), errors.Trace(mh.err)
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler interface.
func (v *mvccValue) UnmarshalBinary(data []byte) error {
	var mh marshalHelper
	buf := bytes.NewBuffer(data)
	var vt int64
	mh.ReadNumber(buf, &vt)
	v.valueType = mvccValueType(vt)
	mh.ReadNumber(buf, &v.startTS)
	mh.ReadNumber(buf, &v.commitTS)
	mh.ReadSlice(buf, &v.value)
	return errors.Trace(mh.err)
}

type marshalHelper struct {
	err error
}

func (mh *marshalHelper) WriteSlice(buf io.Writer, slice []byte) {
	if mh.err != nil {
		return
	}
	var tmp [binary.MaxVarintLen64]byte
	off := binary.PutUvarint(tmp[:], uint64(len(slice)))
	if err := writeFull(buf, tmp[:off]); err != nil {
		mh.err = errors.Trace(err)
		return
	}
	if err := writeFull(buf, slice); err != nil {
		mh.err = errors.Trace(err)
	}
}

func (mh *marshalHelper) WriteNumber(buf io.Writer, n interface{}) {
	if mh.err != nil {
		return
	}
	err := binary.Write(buf, binary.LittleEndian, n)
	if err != nil {
		mh.err = errors.Trace(err)
	}
}

func writeFull(w io.Writer, slice []byte) error {
	written := 0
	for written < len(slice) {
		n, err := w.Write(slice[written:])
		if err != nil {
			return errors.Trace(err)
		}
		written += n
	}
	return nil
}

func (mh *marshalHelper) ReadNumber(r io.Reader, n interface{}) {
	if mh.err != nil {
		return
	}
	err := binary.Read(r, binary.LittleEndian, n)
	if err != nil {
		mh.err = errors.Trace(err)
	}
}

func (mh *marshalHelper) ReadSlice(r *bytes.Buffer, slice *[]byte) {
	if mh.err != nil {
		return
	}
	sz, err := binary.ReadUvarint(r)
	if err != nil {
		mh.err = errors.Trace(err)
		return
	}
	const c10M = 10 * 1024 * 1024
	if sz > c10M {
		mh.err = errors.New("too large slice, maybe something wrong")
		return
	}
	data := make([]byte, sz)
	if _, err := io.ReadFull(r, data); err != nil {
		mh.err = errors.Trace(err)
		return
	}
	*slice = data
}

// lockErr returns ErrLocked.
// Note that parameter key is raw key, while key in ErrLocked is mvcc key.
func (l *mvccLock) lockErr(key []byte) error {
	return &ErrLocked{
		Key:         mvccEncode(key, lockVer),
		Primary:     l.primary,
		StartTS:     l.startTS,
		ForUpdateTS: l.forUpdateTS,
		TTL:         l.ttl,
		TxnSize:     l.txnSize,
		LockType:    l.op,
	}
}

func (l *mvccLock) check(ts uint64, key []byte, resolvedLocks []uint64) (uint64, error) {
	// ignore when ts is older than lock or lock's type is Lock.
	// Pessimistic lock doesn't block read.
	if l.startTS > ts || l.op == kvrpcpb.Op_Lock || l.op == kvrpcpb.Op_PessimisticLock {
		return ts, nil
	}
	// for point get latest version.
	if ts == math.MaxUint64 && bytes.Equal(l.primary, key) {
		return l.startTS - 1, nil
	}
	// Skip lock if the lock is resolved.
	for _, resolved := range resolvedLocks {
		if l.startTS == resolved {
			return ts, nil
		}
	}
	return 0, l.lockErr(key)
}

func (e *mvccEntry) Less(than btree.Item) bool {
	return bytes.Compare(e.key, than.(*mvccEntry).key) < 0
}

func (e *mvccEntry) Get(ts uint64, isoLevel kvrpcpb.IsolationLevel, resolvedLocks []uint64) ([]byte, error) {
	if isoLevel == kvrpcpb.IsolationLevel_SI && e.lock != nil {
		var err error
		ts, err = e.lock.check(ts, e.key.Raw(), resolvedLocks)
		if err != nil {
			return nil, err
		}
	}
	for _, v := range e.values {
		if v.commitTS <= ts && v.valueType != typeRollback && v.valueType != typeLock {
			return v.value, nil
		}
	}
	return nil, nil
}

type rawEntry struct {
	key   []byte
	value []byte
}

func (e *rawEntry) Less(than btree.Item) bool {
	return bytes.Compare(e.key, than.(*rawEntry).key) < 0
}

// MVCCStore is a mvcc key-value storage.
type MVCCStore = mocktikv.MVCCStore

// RawKV is a key-value storage. MVCCStore can be implemented upon it with timestamp encoded into key.
type RawKV = mocktikv.RawKV

// MVCCDebugger is for debugging.
type MVCCDebugger = mocktikv.MVCCDebugger

// Pair is a KV pair read from MvccStore or an error if any occurs.
type Pair =	mocktikv.Pair

func regionContains(startKey []byte, endKey []byte, key []byte) bool {
	return bytes.Compare(startKey, key) <= 0 &&
		(bytes.Compare(key, endKey) < 0 || len(endKey) == 0)
}

// MvccKey is the encoded key type.
// On TiKV, keys are encoded before they are saved into storage engine.
type MvccKey []byte

// NewMvccKey encodes a key into MvccKey.
func NewMvccKey(key []byte) MvccKey {
	if len(key) == 0 {
		return nil
	}
	return codec.EncodeBytes(nil, key)
}

// Raw decodes a MvccKey to original key.
func (key MvccKey) Raw() []byte {
	if len(key) == 0 {
		return nil
	}
	_, k, err := codec.DecodeBytes(key, nil)
	if err != nil {
		panic(err)
	}
	return k
}
