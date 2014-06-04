package main

import (
	"bytes"

	levigo "github.com/jmhodges/levigo"
)

type LevelDB struct {
	db    *levigo.DB
	opts  *levigo.Options
	wopts *levigo.WriteOptions
	ropts *levigo.ReadOptions
	cache *levigo.Cache
}

func NewLeveDb(path string) (LevelDB, error) {
	opts := levigo.NewOptions()
	cache := levigo.NewLRUCache(100 * 1024 * 1024)
	opts.SetCompression(levigo.NoCompression)
	opts.SetCache(cache)
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open(path, opts)
	wopts := levigo.NewWriteOptions()
	ropts := levigo.NewReadOptions()
	return LevelDB{db, opts, wopts, ropts, cache}, err
}

func (db LevelDB) Close() {
	db.cache.Close()
	db.ropts.Close()
	db.wopts.Close()
	db.opts.Close()
	db.db.Close()
}

func (db LevelDB) Write(key, value []byte) error {
	return db.BatchWrite([]Write{Write{key, value}})
}

func (db LevelDB) BatchWrite(writes []Write) error {
	wb := levigo.NewWriteBatch()
	defer wb.Close()
	for _, w := range writes {
		wb.Put(w.Key, w.Value)
	}
	return db.db.Write(db.wopts, wb)
}

func (db LevelDB) Del(start, finish []byte) error {
	wb := levigo.NewWriteBatch()
	defer wb.Close()

	itr, err := db.GetRange(start, finish)
	if err != nil {
		return err
	}
	defer itr.Close()
	for {
		cont, err := itr.Next()
		if err != nil {
			return err
		}

		if !cont {
			break
		}

		k, err := itr.GetKey()
		if err != nil {
			return err
		}
		wb.Delete(k)
	}

	return db.db.Write(db.wopts, wb)
}

type LevelDbIterator struct {
	_itr    *levigo.Iterator
	start   []byte
	end     []byte
	started bool
}

func (itr *LevelDbIterator) Next() (bool, error) {
	if itr.started {
		itr._itr.Next()
	} else {
		itr.started = true
		itr._itr.Seek(itr.start)
	}

	if !itr._itr.Valid() {
		return false, itr._itr.GetError()
	}
	return bytes.Compare(itr._itr.Key(), itr.end) < 1, nil
}

func (itr *LevelDbIterator) GetKey() ([]byte, error) {
	return itr._itr.Key(), nil
}

func (itr *LevelDbIterator) GetValue() ([]byte, error) {
	return itr._itr.Value(), nil
}

func (itr *LevelDbIterator) Close() error {
	itr._itr.Close()
	return nil
}

func (db LevelDB) GetRange(start, finish []byte) (Iterator, error) {
	itr := db.db.NewIterator(db.ropts)

	return &LevelDbIterator{itr, start, finish, false}, nil
}
