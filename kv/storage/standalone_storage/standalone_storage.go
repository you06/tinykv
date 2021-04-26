package standalone_storage

import (
	"errors"
	"sync/atomic"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	active int64
	dbPath string
	db     *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	s := &StandAloneStorage{
		dbPath: conf.DBPath,
		db:     nil,
	}

	return s
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).

	// avoid twice starting engine
	if s.db != nil {
		return errors.New("storage engine is already started")
	}

	opt := badger.DefaultOptions
	opt.Dir = s.dbPath
	opt.ValueDir = s.dbPath
	db, err := badger.Open(opt)
	if err == nil {
		s.db = db
	}
	return err
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).

	// avoid void dereference when db engine is not running
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	if err == nil {
		s.db = nil
	}
	return err
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).

	// TODO: handle engine closed

	txn := s.db.NewTransaction(false)

	atomic.AddInt64(&s.active, 1)
	return &Reader{
		s:   s,
		txn: txn,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).

	var (
		err error
		txn = s.db.NewTransaction(true)
	)
	defer txn.Discard()
	for _, item := range batch {
		cfKey := engine_util.KeyWithCF(item.Cf(), item.Key())
		value := item.Value()
		if value == nil {
			err = txn.Delete(cfKey)
		} else {
			err = txn.Set(cfKey, item.Value())
		}
		if err != nil {
			return err
		}
	}

	return txn.Commit()
}

type Reader struct {
	s   *StandAloneStorage
	txn *badger.Txn
}

func (r *Reader) GetCF(cf string, key []byte) ([]byte, error) {
	cfKey := engine_util.KeyWithCF(cf, key)
	item, err := r.txn.Get(cfKey)
	if err != nil {
		// handle key not found
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	return item.Value()
}

func (r *Reader) IterCF(cf string) engine_util.DBIterator {
	opt := badger.IteratorOptions{}
	opt.StartKey, opt.EndKey = CFRange(cf)
	iter := r.txn.NewIterator(opt)
	iter.Rewind()
	return &Iterator{
		cf:   cf,
		iter: iter,
	}
}

func (r *Reader) Close() {
	r.txn.Discard()
}

func CFRange(cf string) ([]byte, []byte) {
	startKey := engine_util.KeyWithCF(cf, nil)
	endKey := make([]byte, len(startKey))
	copy(endKey, startKey)
	endKey[len(endKey)-2] += 1
	return startKey, endKey
}

type Iterator struct {
	cf   string
	iter *badger.Iterator
}

func (i *Iterator) Item() engine_util.DBItem {
	item := i.iter.Item()
	return &Item{
		cf:   i.cf,
		item: item,
	}
}

func (i *Iterator) Valid() bool {
	return i.iter.Valid()
}

func (i *Iterator) Next() {
	i.iter.Next()
}

func (i *Iterator) Seek(key []byte) {
	i.iter.Seek(key)
}

func (i *Iterator) Close() {
	i.iter.Close()
}

type Item struct {
	cf   string
	item *badger.Item
}

func (i *Item) Key() []byte {
	return StripCF(i.cf, i.item.Key())
}

func (i *Item) KeyCopy(dst []byte) []byte {
	key := StripCF(i.cf, i.item.Key())
	return y.SafeCopy(dst, key)
}

func (i *Item) Value() ([]byte, error) {
	return i.item.Value()
}

func (i *Item) ValueSize() int {
	return i.item.ValueSize()
}

func (i *Item) ValueCopy(dst []byte) ([]byte, error) {
	return i.item.ValueCopy(dst)
}

func StripCF(cf string, key []byte) []byte {
	cfBytes := []byte(cf)
	if len(key) <= len(cfBytes) {
		return key
	}
	for i, b := range cfBytes {
		if key[i] != b {
			return key
		}
	}
	if key[len(cfBytes)] != '_' {
		return key
	}
	return key[len(cfBytes)+1:]
}
