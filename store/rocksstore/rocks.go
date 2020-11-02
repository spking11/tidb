package rocksstore

import(
	"net/url"
	"strings"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
)

// Driver is a test rocksdb driver.
type Driver struct{}

// Open creates a EocksStore storage.
func (d Driver) Open(path string)(kv.Storage,error){
	u, err := url.Parse(path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !strings.EqualFold(u.Scheme, "rocksstore") {
		return nil, errors.Errorf("Uri scheme expected(rocksstore) but found (%s)", u.Scheme)
	}

	mvccStore, err := NewMVCCRocksDB(u.Path)//TODO
	if err != nil {
		return nil, errors.Trace(err)
	}
	cluster := mocktikv.NewCluster(mvccStore)
	client := mocktikv.NewRPCClient(cluster, mvccStore)
	pdClient := mocktikv.NewPDClient(cluster)
	mocktikv.BootstrapWithSingleStore(cluster)
	return tikv.NewTestTiKVStore(client, pdClient, nil, nil, 0)
}	
