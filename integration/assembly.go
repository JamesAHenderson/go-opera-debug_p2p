package integration

import (
	"crypto/ecdsa"
	"fmt"
	"github.com/Fantom-foundation/go-lachesis/utils/adapters/vecmt2dagidx"
	"github.com/Fantom-foundation/go-lachesis/vecmt"
	"github.com/Fantom-foundation/lachesis-base/abft"
	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/flushable"
	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"

	"github.com/Fantom-foundation/go-lachesis/app"
	"github.com/Fantom-foundation/go-lachesis/gossip"
)

func panics(name string) func(error) {
	return func(err error) {
		log.Crit(fmt.Sprintf("%s error", name), "err", err)
	}
}

type GossipStoreAdapter struct {
	*gossip.Store
}

func (g *GossipStoreAdapter) GetEvent(id hash.Event) dag.Event {
	e := g.Store.GetEvent(id)
	if e == nil {
		return nil
	}
	return e
}

// MakeEngine makes consensus engine from config.
func MakeEngine(dataDir string, gossipCfg *gossip.Config) (*abft.Lachesis, *vecmt.Index, *flushable.SyncedPool, *gossip.Store) {
	dbs := flushable.NewSyncedPool(DBProducer(dataDir))

	appStoreConfig := app.StoreConfig{
		ReceiptsCacheSize:    gossipCfg.ReceiptsCacheSize,
		StakersCacheSize:     gossipCfg.StakersCacheSize,
	}
	gdb := gossip.NewStore(dbs, gossipCfg.StoreConfig, appStoreConfig)

	cMainDb := dbs.GetDb("lachesis")
	cGetEpochDB := func(epoch idx.Epoch) kvdb.DropableStore {
		return dbs.GetDb(fmt.Sprintf("lachesis-%d", epoch))
	}
	cdb := abft.NewStore(cMainDb, cGetEpochDB, panics("Lachesis store"), abft.DefaultStoreConfig())

	// write genesis

	err := gdb.Migrate()
	if err != nil {
		utils.Fatalf("Failed to migrate Gossip DB: %v", err)
	}
	genesisAtropos, _, isNew, err := gdb.ApplyGenesis(&gossipCfg.Net)
	if err != nil {
		utils.Fatalf("Failed to write Gossip genesis state: %v", err)
	}

	if isNew {
		err = cdb.ApplyGenesis(&abft.Genesis{
			Epoch:      gdb.GetEpoch(),
			Validators: gossipCfg.Net.Genesis.Alloc.Validators.Build(),
			Atropos:    genesisAtropos,
		})
		if err != nil {
			utils.Fatalf("Failed to write Miniopera genesis state: %v", err)
		}
	}

	err = dbs.Flush(genesisAtropos.Bytes())
	if err != nil {
		utils.Fatalf("Failed to flush genesis state: %v", err)
	}

	if isNew {
		log.Info("Applied genesis state", "hash", genesisAtropos.FullID())
	} else {
		log.Info("Genesis state is already written", "hash", genesisAtropos.FullID())
	}

	// create consensus
	vecClock := vecmt.NewIndex(panics("Vector clock"), vecmt.DefaultConfig())
	engine := abft.NewLachesis(cdb, &GossipStoreAdapter{gdb}, vecmt2dagidx.Wrap(vecClock), panics("Lachesis"), abft.DefaultConfig())

	return engine, vecClock, dbs, gdb
}

// SetAccountKey sets key into accounts manager and unlocks it with pswd.
func SetAccountKey(
	am *accounts.Manager, key *ecdsa.PrivateKey, pswd string,
) (
	acc accounts.Account,
) {
	kss := am.Backends(keystore.KeyStoreType)
	if len(kss) < 1 {
		log.Warn("Keystore is not found")
		return
	}
	ks := kss[0].(*keystore.KeyStore)

	acc = accounts.Account{
		Address: crypto.PubkeyToAddress(key.PublicKey),
	}

	imported, err := ks.ImportECDSA(key, pswd)
	if err == nil {
		acc = imported
	} else if err.Error() != "account already exists" {
		log.Crit("Failed to import key", "err", err)
	}

	err = ks.Unlock(acc, pswd)
	if err != nil {
		log.Crit("failed to unlock key", "err", err)
	}

	return
}
