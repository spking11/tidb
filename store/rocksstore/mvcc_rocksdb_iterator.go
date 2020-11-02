package rocksstore
import (
	"github.com/pingcap/errors"
	"github.com/spking11/gorocksdb"
)
// seet to Start, limit to Limit
func newIterator(db *gorocksdb.DB, slice *gorocksdb.Range) *gorocksdb.Iterator {
	readOpts := gorocksdb.NewDefaultReadOptions()
	if(slice.Limit != nil){
		readOpts.SetIterateUpperBound(slice.Limit)
	}
	iter := db.NewIterator(readOpts)

	if(slice.Start != nil){
		iter.Seek(slice.Start)
	}else{
		iter.SeekToFirst()
	}

	return iter
}

func newScanIterator(db *gorocksdb.DB, startKey, endKey []byte) (*gorocksdb.Iterator, []byte, error) {
	var start, end []byte
	if len(startKey) > 0 {
		start = mvccEncode(startKey, lockVer)
	}
	if len(endKey) > 0 {
		end = mvccEncode(endKey, lockVer)
	}
	iter := newIterator(db, &gorocksdb.Range{
		Start: start,
		Limit: end,
	})
	// newScanIterator must handle startKey is nil, in this case, the real startKey
	// should be change the frist key of the store.
	if len(startKey) == 0 && iter.Valid() {
		key, _, err := mvccDecode(iter.Key().Data())
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		startKey = key
	}
	return iter, startKey, nil
}
