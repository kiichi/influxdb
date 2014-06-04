package main

import (
	"bytes"
	"fmt"
	"os"

	mdb "github.com/szferi/gomdb"
)

type Mdb struct {
	env *mdb.Env
	db  mdb.DBI
}

func NewMDB(path, name string) (Mdb, error) {
	env, err := mdb.NewEnv()
	if err != nil {
		return Mdb{}, err
	}

	// TODO: max dbs should be configurable
	if err := env.SetMaxDBs(10); err != nil {
		return Mdb{}, err
	}
	if err := env.SetMapSize(10 << 30); err != nil {
		return Mdb{}, err
	}

	if _, err := os.Stat(path); err != nil {
		err = os.MkdirAll(path, 0755)
		if err != nil {
			return Mdb{}, err
		}
	}

	err = env.Open(path, mdb.WRITEMAP|mdb.MAPASYNC|mdb.CREATE, 0755)
	if err != nil {
		return Mdb{}, err
	}

	tx, err := env.BeginTxn(nil, 0)
	if err != nil {
		return Mdb{}, err
	}

	dbi, err := tx.DBIOpen(&name, mdb.CREATE)
	if err != nil {
		return Mdb{}, err
	}

	if err := tx.Commit(); err != nil {
		return Mdb{}, err
	}

	db := Mdb{
		env: env,
		db:  dbi,
	}

	return db, nil
}

func (db Mdb) Write(key, value []byte) error {
	return db.BatchWrite([]Write{Write{key, value}})
}

func (db Mdb) BatchWrite(writes []Write) error {
	tx, err := db.env.BeginTxn(nil, 0)
	if err != nil {
		return err
	}

	for _, w := range writes {
		if err := tx.Put(db.db, w.Key, w.Value, 0); err != nil {
			tx.Abort()
			return err
		}
	}

	return tx.Commit()
}

func (db Mdb) Del(start, finish []byte) error {
	tx, err := db.env.BeginTxn(nil, 0)
	if err != nil {
		return err
	}
	defer tx.Commit()

	itr, err := db.getRange(start, finish, true)
	if err != nil {
		return err
	}
	defer itr.Close()

	count := 0
	for {
		cont, err := itr.next(true)
		if err != nil {
			return err
		}
		if !cont {
			break
		}

		// TODO: We should be using one cursor instead of two
		// transactions, but deleting using a cursor, crashes
		err = tx.Del(db.db, itr.key, nil)
		if err != nil {
			return err
		}
		count++
	}
	fmt.Printf("Deleted %d points\n", count)
	return nil
}

type MdbIterator struct {
	key     []byte
	value   []byte
	start   []byte
	end     []byte
	c       *mdb.Cursor
	tx      *mdb.Txn
	started bool
}

func (itr *MdbIterator) GetKey() ([]byte, error) {
	return itr.key, nil
}

func (itr *MdbIterator) GetValue() ([]byte, error) {
	return itr.value, nil
}

func (itr *MdbIterator) getCurrent() ([]byte, []byte, error) {
	return itr.c.Get(nil, mdb.GET_CURRENT)
}

func (itr *MdbIterator) Next() (bool, error) {
	return itr.next(true)
}

func (itr *MdbIterator) next(advance bool) (bool, error) {
	var err error
	if !itr.started {
		itr.started = true
		itr.key, itr.value, err = itr.c.Get(itr.start, mdb.SET_RANGE)
	} else if advance {
		itr.key, itr.value, err = itr.c.Get(nil, mdb.NEXT)
	} else {
		itr.key, itr.value, err = itr.c.Get(nil, mdb.GET_CURRENT)
	}
	if err == mdb.NotFound {
		return false, nil
	}
	return bytes.Compare(itr.key, itr.end) < 1, err
}

func (itr *MdbIterator) Close() error {
	if err := itr.c.Close(); err != nil {
		return err
	}
	return itr.tx.Commit()
}

func (db Mdb) GetRange(start, finish []byte) (Iterator, error) {
	return db.getRange(start, finish, true)
}

func (db Mdb) getRange(start, finish []byte, rdonly bool) (*MdbIterator, error) {
	flags := uint(0)
	if rdonly {
		flags = mdb.RDONLY
	}
	tx, err := db.env.BeginTxn(nil, flags)
	if err != nil {
		return nil, err
	}

	c, err := tx.CursorOpen(db.db)
	if err != nil {
		return nil, err
	}

	return &MdbIterator{nil, nil, start, finish, c, tx, false}, nil
}

func (db Mdb) Close() error {
	db.env.DBIClose(db.db)
	if err := db.env.Close(); err != nil {
		panic(err)
	}
	return nil
}
