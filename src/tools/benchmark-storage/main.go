package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	points := flag.Int("points", 200000000, "Number of points")
	batchSize := flag.Int("batch", 1000, "Batch size")
	series := flag.Int("series", 1, "Number of series")
	flag.Parse()

	os.RemoveAll("/tmp/test-ldb")
	os.RemoveAll("/tmp/test-mdb")

	benchmarkLevelDb(Config{*points, *batchSize, *series, 0, 0, time.Now()})
	benchmarkMdb(Config{*points, *batchSize, *series, 0, 0, time.Now()})
}

func benchmarkMdb(c Config) {
	db, err := NewMDB("/tmp/test-mdb", "test")

	if err != nil {
		panic(err)
	}

	defer db.Close()

	benchmarkDbCommon(db, c)
}

func benchmarkLevelDb(c Config) {
	db, err := NewLeveDb("/tmp/test-ldb")

	if err != nil {
		panic(err)
	}

	defer db.Close()

	benchmarkDbCommon(db, c)
}

func benchmarkDbCommon(db Db, c Config) {
	start := time.Now()
	for p := c.points; p > 0; {
		p -= writeBatch(db, &c)
	}
	d := time.Now().Sub(start)
	fmt.Printf("Writing %d points in batches of %d points took %s (%f microsecond per point)\n", c.points, c.batch, d, float64(d.Nanoseconds())/1000.0/float64(c.points))

	for i := int64(c.series) - 1; i > 0; i-- {
		queryAndDelete(db, i, &c)
	}
}

func queryAndDelete(db Db, s int64, c *Config) {
	// query the database
	startCount := c.points / c.series / 4
	endCount := c.points * 3 / c.series / 4

	var delStart []byte
	var delEnd []byte

	count := 0

	query(db, s, func(itr Iterator) {
		count++

		var err error
		if count == startCount {
			delStart, err = itr.GetKey()
		}
		if count == endCount-1 {
			delEnd, err = itr.GetKey()
		}
		if err != nil {
			panic(err)
		}
	})

	start := time.Now()
	err := db.Del(delStart, delEnd)
	if err != nil {
		panic(err)
	}

	d := time.Now().Sub(start)
	fmt.Printf("Took %s to delete data\n", d)

	query(db, s, func(_ Iterator) {})
}

func query(db Db, s int64, yield func(Iterator)) {
	fmt.Printf("Querying series %d\n", s)
	sb := bytes.NewBuffer(nil)
	binary.Write(sb, binary.BigEndian, s)
	binary.Write(sb, binary.BigEndian, int64(0))
	binary.Write(sb, binary.BigEndian, int64(0))
	eb := bytes.NewBuffer(nil)
	binary.Write(eb, binary.BigEndian, s)
	binary.Write(eb, binary.BigEndian, int64(-1))
	binary.Write(eb, binary.BigEndian, int64(-1))
	itr, err := db.GetRange(sb.Bytes(), eb.Bytes())
	if err != nil {
		panic(err)
	}

	count := 0
	start := time.Now()
	for {
		cont, err := itr.Next()
		if err != nil {
			panic(err)
		}

		if !cont {
			break
		}

		count++
		yield(itr)
	}

	itr.Close()
	d := time.Now().Sub(start)
	fmt.Printf("Took %s to query %d points (%f microseconds per point)\n", d, count, float64(d.Nanoseconds())/1000.0/float64(count))
}

func writeBatch(db Db, c *Config) int {
	ws := c.MakeBatch()
	if err := db.BatchWrite(ws); err != nil {
		panic(err)
	}
	return len(ws)
}
