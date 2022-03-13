package integration

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
)

type DBProducerWithSummary struct {
	kvdb.FlushableDBProducer

	written []uint64
	erased  []uint64
	start   time.Time
}

type DropableStoreWithSummary struct {
	kvdb.DropableStore

	written    []uint64
	erased     []uint64
	start      time.Time
	lastLogged time.Time
	log        io.Writer
}

type BatchWithSummary struct {
	kvdb.Batch

	written []uint64
	erased  []uint64
}

func WrapDatabaseWithSummary(db kvdb.FlushableDBProducer) kvdb.FlushableDBProducer {
	wrapper := &DBProducerWithSummary{
		FlushableDBProducer: db,
		written:             make([]uint64, 256),
		erased:              make([]uint64, 256),
		start:               time.Now(),
	}
	return wrapper
}

func (ds *DropableStoreWithSummary) logTick() {
	if time.Since(ds.lastLogged) > time.Second*60 {
		ds.lastLogged = time.Now()
		fmt.Fprintf(ds.log, "\nLogging at %s\n", ds.lastLogged.String())
		fmt.Fprintf(ds.log, "Written:\n")
		for i := 0; i < 256; i++ {
			if ds.written[i] == 0 {
				continue
			}
			fmt.Fprintf(ds.log, "0x%02x: %d\n", i, ds.written[i])
			ds.written[i] = 0
		}
		fmt.Fprintf(ds.log, "Erased:\n")
		for i := 0; i < 256; i++ {
			if ds.erased[i] == 0 {
				continue
			}
			fmt.Fprintf(ds.log, "0x%02x: %d\n", i, ds.erased[i])
			ds.erased[i] = 0
		}
	}
}

func (ds *DropableStoreWithSummary) Put(key, val []byte) error {
	ds.logTick()
	ds.written[key[0]]++
	return ds.DropableStore.Put(key, val)
}

func (ds *DropableStoreWithSummary) Delete(key []byte) error {
	ds.logTick()
	ds.erased[key[0]]++
	return ds.DropableStore.Delete(key)
}

func (ds *DropableStoreWithSummary) NewBatch() kvdb.Batch {
	return &BatchWithSummary{
		Batch:   ds.DropableStore.NewBatch(),
		written: ds.written,
		erased:  ds.erased,
	}
}

func (ds *BatchWithSummary) Put(key, val []byte) error {
	ds.written[key[0]]++
	return ds.Batch.Put(key, val)
}

func (ds *BatchWithSummary) Delete(key []byte) error {
	ds.erased[key[0]]++
	return ds.Batch.Delete(key)
}

func (db *DBProducerWithSummary) OpenDB(name string) (kvdb.DropableStore, error) {
	ds, err := db.FlushableDBProducer.OpenDB(name)
	if err != nil {
		return nil, err
	}

	_ = os.MkdirAll("/tmp/operadblogs/", 0700)
	f, _ := os.Create("/tmp/operadblogs/" + name)
	return &DropableStoreWithSummary{
		DropableStore: ds,
		written:       db.written,
		erased:        db.erased,
		start:         db.start,
		log:           f,
	}, nil
}
