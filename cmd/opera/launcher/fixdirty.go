package launcher

import (
	"bytes"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/Fantom-foundation/lachesis-base/abft"
	"github.com/Fantom-foundation/lachesis-base/common/bigendian"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/flushable"
	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/log"
	"gopkg.in/urfave/cli.v1"

	"github.com/Fantom-foundation/go-opera/integration"
)

var (
	fixDirtyCommand = cli.Command{
		Action:      utils.MigrateFlags(fixDirty),
		Name:        "fixdirty",
		Usage:       "Experimental - try to fix dirty DB",
		ArgsUsage:   "",
		Flags:       append(nodeFlags, testFlags...),
		Category:    "MISCELLANEOUS COMMANDS",
		Description: `Experimental - try to fix dirty DB.`,
	}
)

// fixDirty is the fixdirty command.
func fixDirty(ctx *cli.Context) error {
	cfg := makeAllConfigs(ctx)

	log.Info("Opening databases")
	producer := integration.DBProducer(path.Join(cfg.Node.DataDir, "chaindata"), cfg.cachescale)
	gdb, err := makeRawGossipStore(producer, cfg) // requires FlushIDKey present in all dbs
	if err != nil {
		log.Crit("DB opening error", "datadir", cfg.Node.DataDir, "err", err)
	}

	// we try to revert to the previous epoch end
	log.Info("Finding last closed epoch...")
	epochIdx := gdb.GetEpoch() - 1
	blockState, epochState := gdb.GetHistoryBlockEpochState(epochIdx)
	if blockState == nil || epochState == nil {
		return fmt.Errorf("epoch %d is not available", epochIdx)
	}
	if !gdb.EvmStore().HasStateDB(blockState.FinalizedStateRoot) {
		return fmt.Errorf("state for epoch %d is not available", epochIdx)
	}
	// set the historic state to be the current
	log.Info("Setting block epoch state")
	gdb.SetBlockEpochState(*blockState, *epochState)
	gdb.FlushBlockEpochState()

	// Service.switchEpochTo
	gdb.SetHighestLamport(0)
	gdb.FlushHighestLamport()

	// drop epoch databases
	log.Info("Removing epoch dbs")
	err = dropAllEpochDbs(producer)
	if err != nil {
		return err
	}

	// drop consensus database
	log.Info("Removing lachesis db")
	cMainDb := mustOpenDB(producer, "lachesis")
	_ = cMainDb.Close()
	cMainDb.Drop()

	// prepare consensus database from epochState
	log.Info("Recreating lachesis db")
	cMainDb = mustOpenDB(producer, "lachesis")
	cGetEpochDB := func(epoch idx.Epoch) kvdb.DropableStore {
		return mustOpenDB(producer, fmt.Sprintf("lachesis-%d", epoch))
	}
	cdb := abft.NewStore(cMainDb, cGetEpochDB, panics("Lachesis store"), cfg.LachesisStore)
	err = cdb.ApplyGenesis(&abft.Genesis{
		Epoch:      epochState.Epoch,
		Validators: epochState.Validators,
	})
	if err != nil {
		return fmt.Errorf("failed to init consensus database: %v", err)
	}
	_ = cdb.Close()
	gdb.Close()

	log.Info("Clearing dbs dirty flags")
	err = clearDirtyFlags(producer)
	if err != nil {
		return err
	}

	log.Info("Fixing done")
	return nil
}

func dropAllEpochDbs(producer kvdb.IterableDBProducer) error {
	for _, name := range producer.Names() {
		if (strings.HasPrefix(name, "gossip-") || strings.HasPrefix(name, "lachesis-")) && name != "gossip-async" {
			log.Info("Removing db", "name", name)
			db, err := producer.OpenDB(name)
			if err != nil {
				return fmt.Errorf("unable to open db %s; %s", name, err)
			}
			_ = db.Close()
			log.Info("Dropping", "name", name)
			db.Drop()
		}
	}
	return nil
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

		mark, err := db.Get(integration.FlushIDKey)
		if err != nil {
			return err
		}
		if bytes.HasPrefix(mark, []byte{flushable.DirtyPrefix}) {
			log.Info("Found dirty state - fixing", "name", name)
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

func mustOpenDB(producer kvdb.DBProducer, name string) kvdb.DropableStore {
	db, err := producer.OpenDB(name)
	if err != nil {
		utils.Fatalf("Failed to open '%s' database: %v", name, err)
	}
	return db
}

func panics(name string) func(error) {
	return func(err error) {
		log.Crit(fmt.Sprintf("%s error", name), "err", err)
	}
}
