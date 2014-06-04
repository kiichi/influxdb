package main

type Write struct {
	Key   []byte
	Value []byte
}

type Iterator interface {
	GetKey() ([]byte, error)
	GetValue() ([]byte, error)
	Next() (bool, error)
	Close() error
}

type Db interface {
	Write(key, value []byte) error
	BatchWrite(writes []Write) error
	Del(start, finish []byte) error
	GetRange(start, finish []byte) (Iterator, error)
}
