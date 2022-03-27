package launcher

import (
	"os"
	"path"
	"strings"
	"time"

	"github.com/Fantom-foundation/lachesis-base/common/bigendian"
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/batched"
	"github.com/Fantom-foundation/lachesis-base/kvdb/cachedproducer"
	"github.com/Fantom-foundation/lachesis-base/kvdb/flushable"
	"github.com/Fantom-foundation/lachesis-base/kvdb/multidb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/table"
	"github.com/ethereum/go-ethereum/log"
	"gopkg.in/urfave/cli.v1"

	"github.com/Fantom-foundation/go-opera/integration"
)

func dbMigrate(ctx *cli.Context) error {
	cfg := makeAllConfigs(ctx)

	// get supported DB producers
	dbTypes := getDBProducersFor(cfg, path.Join(cfg.Node.DataDir, "chaindata"))

	byReq, err := readRoutes(cfg, dbTypes)
	if err != nil {
		log.Crit("Failed to read routes", "err", err)
	}
	byDB := separateIntoDBs(byReq)

	// weed out DBs which don't need migration
	{
		for _, byReqOfDB := range byDB {
			match := true
			for _, e := range byReqOfDB {
				if e.Old != e.New {
					match = false
					break
				}
			}
			if match {
				for _, e := range byReqOfDB {
					delete(byReq, e.Req)
				}
			}
		}
	}
	if len(byReq) == 0 {
		log.Info("No DB migration is needed")
		return nil
	}

	// check if new layout is contradictory
	for _, e0 := range byReq {
		for _, e1 := range byReq {
			if e0 == e1 {
				continue
			}
			if dbLocatorOf(e0.New) == dbLocatorOf(e1.New) && strings.HasPrefix(e0.New.Table, e1.New.Table) {
				log.Crit("New DB layout is contradictory", "db_type", e0.New.Type, "db_name", e0.New.Name,
					"req0", e0.Req, "req1", e1.Req, "table0", e0.New.Table, "table1", e1.New.Table)
			}
		}
	}

	// separate entries into inter-linked components
	byComponents := make([]map[string]dbMigrationEntry, 0)
	for componentI := 0; len(byReq) > 0; componentI++ {
		var someEntry dbMigrationEntry
		for _, e := range byReq {
			someEntry = e
			break
		}

		// DFS
		component := make(map[string]dbMigrationEntry)
		stack := make(dbMigrationEntries, 0)
		for pwalk := &someEntry; pwalk != nil; pwalk = stack.Pop() {
			if _, ok := component[pwalk.Req]; ok {
				continue
			}
			component[pwalk.Req] = *pwalk
			delete(byReq, pwalk.Req)
			for _, e := range byDB[dbLocatorOf(pwalk.Old)] {
				stack = append(stack, e)
			}
			for _, e := range byDB[dbLocatorOf(pwalk.New)] {
				stack = append(stack, e)
			}
		}
		byComponents = append(byComponents, component)
	}

	tmpDbTypes := getDBProducersFor(cfg, path.Join(cfg.Node.DataDir, "tmp"))
	for _, component := range byComponents {
		err := migrateComponent(cfg.Node.DataDir, dbTypes, tmpDbTypes, component)
		if err != nil {
			log.Crit("Failed to migrate component", "err", err)
		}
		// drop unused DBs
		used := make(map[multidb.DBLocator]bool)
		for _, e := range component {
			used[dbLocatorOf(e.New)] = true
		}
		dropped := make(map[multidb.DBLocator]bool)
		for _, e := range component {
			if used[dbLocatorOf(e.Old)] || dropped[dbLocatorOf(e.Old)] {
				continue
			}
			dropped[dbLocatorOf(e.Old)] = true
			log.Info("Dropping unused DB", "db_type", e.Old.Type, "db_name", e.Old.Name)
			deletePath := path.Join(cfg.Node.DataDir, "chaindata", string(e.Old.Type), e.Old.Name)
			err := os.RemoveAll(deletePath)
			if err != nil {
				log.Crit("Failed to erase unused DB", "path", deletePath, "err", err)
			}
		}
	}
	for typ, producer := range dbTypes {
		err := clearDirtyFlags(producer)
		if err != nil {
			log.Crit("Failed to write clean FlushID", "type", typ, "err", err)
		}
	}

	log.Info("DB migration is complete")

	return nil
}

func getDBProducersFor(cfg *config, chaindataDir string) map[multidb.TypeName]kvdb.FullDBProducer {
	dbTypes, err := integration.SupportedDBs(chaindataDir, cfg.DBs.Cache)
	if err != nil {
		log.Crit("Failed to construct DB producers", "err", err)
	}
	wrappedDbTypes := make(map[multidb.TypeName]kvdb.FullDBProducer)
	for typ, producer := range dbTypes {
		wrappedDbTypes[typ] = cachedproducer.WrapAll(&integration.DummyFlushableProducer{IterableDBProducer: producer})
	}
	return wrappedDbTypes
}

type dbMigrationEntry struct {
	Req string
	Old multidb.Route
	New multidb.Route
}

type dbMigrationEntries []dbMigrationEntry

func (ee *dbMigrationEntries) Pop() *dbMigrationEntry {
	l := len(*ee)
	if l == 0 {
		return nil
	}
	res := &(*ee)[l-1]
	*ee = (*ee)[:l-1]
	return res
}

var dbLocatorOf = multidb.DBLocatorOf

var tableLocatorOf = multidb.TableLocatorOf

func readRoutes(cfg *config, dbTypes map[multidb.TypeName]kvdb.FullDBProducer) (map[string]dbMigrationEntry, error) {
	router, err := multidb.NewProducer(dbTypes, cfg.DBs.Routing.Table, integration.TablesKey)
	if err != nil {
		return nil, err
	}
	byReq := make(map[string]dbMigrationEntry)

	for typ, producer := range dbTypes {
		for _, dbName := range producer.Names() {
			db, err := producer.OpenDB(dbName)
			if err != nil {
				log.Crit("DB opening error", "name", dbName, "err", err)
			}
			defer db.Close()
			tables, err := multidb.ReadTablesList(db, integration.TablesKey)
			if err != nil {
				log.Crit("Failed to read tables list", "name", dbName, "err", err)
			}
			for _, t := range tables {
				oldRoute := multidb.Route{
					Type:  typ,
					Name:  dbName,
					Table: t.Table,
				}
				newRoute := router.RouteOf(t.Req)
				newRoute.NoDrop = false
				byReq[t.Req] = dbMigrationEntry{
					Req: t.Req,
					New: newRoute,
					Old: oldRoute,
				}
			}
		}
	}
	return byReq, nil
}

func writeCleanTableRecords(dbTypes map[multidb.TypeName]kvdb.FullDBProducer, byReq map[string]dbMigrationEntry) error {
	records := make(map[multidb.DBLocator][]multidb.TableRecord, 0)
	for _, e := range byReq {
		records[dbLocatorOf(e.New)] = append(records[dbLocatorOf(e.New)], multidb.TableRecord{
			Req:   e.Req,
			Table: e.New.Table,
		})
	}
	written := make(map[multidb.DBLocator]bool)
	for _, e := range byReq {
		if written[dbLocatorOf(e.New)] {
			continue
		}
		written[dbLocatorOf(e.New)] = true

		db, err := dbTypes[e.New.Type].OpenDB(e.New.Name)
		if err != nil {
			return err
		}
		defer db.Close()
		err = multidb.WriteTablesList(db, integration.TablesKey, records[dbLocatorOf(e.New)])
		if err != nil {
			return err
		}
	}
	return nil
}

func migrateComponent(datadir string, dbTypes, tmpDbTypes map[multidb.TypeName]kvdb.FullDBProducer, byReq map[string]dbMigrationEntry) error {
	byDB := separateIntoDBs(byReq)
	// if it can be migrated just by DB renaming
	if len(byDB) == 2 {
		oldDBName := ""
		newDBName := ""
		typ := multidb.TypeName("")
		ok := true
		for _, e := range byReq {
			if len(typ) == 0 {
				oldDBName = e.Old.Name
				newDBName = e.New.Name
				typ = e.New.Type
			}
			if e.Old.Table != e.New.Table || e.New.Name != newDBName || e.Old.Name != oldDBName ||
				e.Old.Type != typ || e.New.Type != typ {
				ok = false
				break
			}
		}
		if ok {
			oldPath := path.Join(datadir, "chaindata", string(typ), oldDBName)
			newPath := path.Join(datadir, "chaindata", string(typ), newDBName)
			log.Info("Renaming DB", "old", oldPath, "new", newPath)
			return os.Rename(oldPath, newPath)
		}
	}

	// check if there's overlapping in tables
	occupied := make(map[multidb.TableLocator]bool)
	overlapping := false
	for _, e := range byReq {
		if occupied[tableLocatorOf(e.Old)] {
			overlapping = true
			break
		}
		if occupied[tableLocatorOf(e.New)] {
			overlapping = true
			break
		}
		occupied[tableLocatorOf(e.Old)] = true
		occupied[tableLocatorOf(e.New)] = true
	}

	// if component only needs moving tables with no overlapping
	if !overlapping {
		for _, e := range byReq {
			if tableLocatorOf(e.New) == tableLocatorOf(e.Old) {
				continue
			}
			err := func() error {
				newDB, err := dbTypes[e.New.Type].OpenDB(e.New.Name)
				if err != nil {
					return err
				}
				newDB = batched.Wrap(newDB)
				defer newDB.Close()
				oldDB, err := dbTypes[e.Old.Type].OpenDB(e.Old.Name)
				if err != nil {
					return err
				}
				oldDB = batched.Wrap(oldDB)
				defer oldDB.Close()
				log.Info("Moving DB table", "req", e.Req, "old_db_type", e.Old.Type, "old_db_name", e.Old.Name, "old_table", e.Old.Table,
					"new_db_type", e.New.Type, "new_db_name", e.New.Name, "new_table", e.New.Table)
				oldTable := table.New(oldDB, []byte(e.Old.Table))
				newTable := table.New(newDB, []byte(e.New.Table))
				it := oldTable.NewIterator(nil, nil)
				defer it.Release()
				for it.Next() {
					err := newTable.Put(it.Key(), it.Value())
					if err != nil {
						return err
					}
					err = oldTable.Delete(it.Key())
					if err != nil {
						return err
					}
				}
				return nil
			}()
			if err != nil {
				return err
			}
		}
		return writeCleanTableRecords(dbTypes, byReq)
	}

	// universal approach: rebuild
	for _, e := range byReq {
		err := func() error {
			oldDB, err := dbTypes[e.Old.Type].OpenDB(e.Old.Name)
			if err != nil {
				return err
			}
			defer oldDB.Close()
			newDB, err := tmpDbTypes[e.New.Type].OpenDB(e.New.Name)
			if err != nil {
				return err
			}
			newDB = batched.Wrap(newDB)
			defer newDB.Close()
			log.Info("Copying DB table", "req", e.Req, "old_db_type", e.Old.Type, "old_db_name", e.Old.Name, "old_table", e.Old.Table,
				"new_db_type", e.New.Type, "new_db_name", "tmp/"+e.New.Name, "new_table", e.Old.Table)
			oldTable := table.New(oldDB, []byte(e.Old.Table))
			newTable := table.New(newDB, []byte(e.New.Table))
			it := oldTable.NewIterator(nil, nil)
			defer it.Release()
			for it.Next() {
				err := newTable.Put(it.Key(), it.Value())
				if err != nil {
					return err
				}
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	dropped := make(map[multidb.DBLocator]bool)
	for _, e := range byReq {
		if dropped[dbLocatorOf(e.Old)] {
			continue
		}
		dropped[dbLocatorOf(e.Old)] = true
		log.Info("Dropping old DB", "db_type", e.Old.Type, "db_name", e.Old.Name)
		deletePath := path.Join(datadir, "chaindata", string(e.Old.Type), e.Old.Name)
		err := os.RemoveAll(deletePath)
		if err != nil {
			return err
		}
	}
	moved := make(map[multidb.DBLocator]bool)
	for _, e := range byReq {
		if moved[dbLocatorOf(e.New)] {
			continue
		}
		moved[dbLocatorOf(e.New)] = true
		oldPath := path.Join(datadir, "tmp", string(e.New.Type), e.New.Name)
		newPath := path.Join(datadir, "chaindata", string(e.New.Type), e.New.Name)
		log.Info("Moving tmp DB to clean dir", "old", oldPath, "new", newPath)
		err := os.Rename(oldPath, newPath)
		if err != nil {
			return err
		}
	}
	return writeCleanTableRecords(dbTypes, byReq)
}

func separateIntoDBs(byReq map[string]dbMigrationEntry) map[multidb.DBLocator]map[string]dbMigrationEntry {
	byDB := make(map[multidb.DBLocator]map[string]dbMigrationEntry)
	for _, e := range byReq {
		if byDB[dbLocatorOf(e.Old)] == nil {
			byDB[dbLocatorOf(e.Old)] = make(map[string]dbMigrationEntry)
		}
		byDB[dbLocatorOf(e.Old)][e.Req] = e
		if byDB[dbLocatorOf(e.New)] == nil {
			byDB[dbLocatorOf(e.New)] = make(map[string]dbMigrationEntry)
		}
		byDB[dbLocatorOf(e.New)][e.Req] = e
	}
	return byDB
}

// clearDirtyFlags - writes the CleanPrefix into all databases
func clearDirtyFlags(rawProducer kvdb.IterableDBProducer) error {
	id := bigendian.Uint64ToBytes(uint64(time.Now().UnixNano()))
	names := rawProducer.Names()
	for _, name := range names {
		db, err := rawProducer.OpenDB(name)
		if err != nil {
			return err
		}

		err = db.Put(integration.FlushIDKey, append([]byte{flushable.CleanPrefix}, id...))
		if err != nil {
			log.Crit("Failed to write CleanPrefix", "name", name)
			return err
		}
		log.Info("Database set clean", "name", name)
		_ = db.Close()
	}
	return nil
}
