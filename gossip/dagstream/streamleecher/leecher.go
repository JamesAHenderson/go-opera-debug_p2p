package streamleecher

import (
	"math/rand"
	"sync"
	"time"

	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"

	"github.com/Fantom-foundation/go-opera/gossip/essteam"
	"github.com/Fantom-foundation/go-opera/gossip/essteam/streamleecher/peerleecher"
)

// Leecher is responsible for requesting events based on lexicographic event streams
type Leecher struct {
	// Callbacks
	callback Callbacks

	cfg Config

	// State
	session sessionState
	epoch   idx.Epoch

	emptyState bool

	peers map[string]struct{}

	quit chan struct{}

	wg sync.WaitGroup

	mu         *sync.RWMutex
	terminated bool
}

// New creates an events downloader to request events based on lexicographic event streams
func New(epoch idx.Epoch, emptyState bool, cfg Config, callback Callbacks) *Leecher {
	return &Leecher{
		cfg:        cfg,
		callback:   callback,
		quit:       make(chan struct{}),
		emptyState: emptyState,
		epoch:      epoch,
		peers:      make(map[string]struct{}),
		mu:         new(sync.RWMutex),
	}
}

type Callbacks struct {
	OnlyNotConnected peerleecher.OnlyNotConnectedFn

	RequestChunk func(peer string, r essteam.Request) error
	Suspend      func(peer string) bool
	PeerEpoch    func(peer string) idx.Epoch
}

type sessionState struct {
	agent        *peerleecher.PeerLeecher
	peer         string
	startTime    time.Time
	endTime      time.Time
	lastReceived time.Time
	try          uint32
}

func (d *Leecher) Start() {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.loop()
	}()
}

func (d *Leecher) shouldTerminateSession() bool {
	if d.session.agent.Stopped() {
		return true
	}

	noProgress := time.Since(d.session.lastReceived) >= d.cfg.BaseProgressWatchdog*time.Duration(d.session.try+5)/5
	stuck := time.Since(d.session.startTime) >= d.cfg.BaseSessionWatchdog*time.Duration(d.session.try+5)/5
	return stuck || noProgress
}

func (d *Leecher) terminateSession() {
	// force the epoch download to end
	if d.session.agent != nil {
		d.session.agent.Terminate()
		d.session.agent = nil
		d.session.endTime = time.Now()
	}
}

func (d *Leecher) selectSessionPeerCandidates() []string {
	var selected []string
	currentEpochPeers := make([]string, 0, len(d.peers))
	futureEpochPeers := make([]string, 0, len(d.peers))
	for p, _ := range d.peers {
		epoch := d.callback.PeerEpoch(p)
		if epoch == d.epoch {
			currentEpochPeers = append(currentEpochPeers, p)
		}
		if epoch > d.epoch {
			futureEpochPeers = append(futureEpochPeers, p)
		}
	}
	sinceEnd := time.Since(d.session.endTime)
	waitUntilProcessed := d.session.try == 0 || sinceEnd > d.cfg.MinSessionRestart
	hasSomethingToSync := len(futureEpochPeers) > 0 || sinceEnd >= d.cfg.MaxSessionRestart
	if waitUntilProcessed && hasSomethingToSync {
		if len(futureEpochPeers) > 0 && (d.session.try%5 != 4 || len(currentEpochPeers) == 0) {
			// normally work only with peers which have a higher epoch
			selected = futureEpochPeers
		} else {
			// if above doesn't work, try peers on current epoch every 5th try
			selected = currentEpochPeers
		}
	}
	return selected
}

func (d *Leecher) getSessionID() uint32 {
	return (uint32(d.epoch) << 12) ^ d.session.try
}

func (d *Leecher) startSession(candidates []string) {
	peer := candidates[rand.Intn(len(candidates))]

	typ := essteam.RequestIDs
	if d.callback.PeerEpoch(peer) > d.epoch && d.emptyState && d.session.try == 0 {
		typ = essteam.RequestEvents
	}

	session := essteam.Session{
		ID:    d.getSessionID(),
		Start: d.epoch.Bytes(),
		Stop:  (d.epoch + 1).Bytes(),
	}

	d.session.agent = peerleecher.New(&d.wg, d.cfg.Session, peerleecher.EpochDownloaderCallbacks{
		OnlyNotConnected: d.callback.OnlyNotConnected,
		RequestChunk: func(n essteam.Metric) error {
			return d.callback.RequestChunk(peer, essteam.Request{session, n, typ})
		},
		Suspend: func() bool {
			return d.callback.Suspend(peer)
		},
		Done: func() bool {
			return false
		},
	})

	d.session.startTime = time.Now()
	d.session.lastReceived = time.Now()
	d.session.endTime = time.Now()
	d.session.try++
	d.session.peer = peer

	d.session.agent.Start()
}

func (d *Leecher) routine() {
	if d.terminated {
		return
	}
	if d.session.agent != nil && d.shouldTerminateSession() {
		d.terminateSession()
	}
	if d.session.agent == nil {
		candidates := d.selectSessionPeerCandidates()
		if len(candidates) != 0 {
			d.startSession(candidates)
		}
	}
}

func (d *Leecher) loop() {
	syncTicker := time.NewTicker(d.cfg.RecheckInterval)
	for {
		select {
		case <-d.quit:
			return
		case <-syncTicker.C:
			d.mu.Lock()
			d.routine()
			d.mu.Unlock()
		}
	}
}

// RegisterPeer injects a new download peer to download epochs from.
func (d *Leecher) RegisterPeer(peer string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.terminated {
		return nil
	}
	d.peers[peer] = struct{}{}

	return nil
}

func (d *Leecher) OnNewEpoch(myEpoch idx.Epoch) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.terminated {
		return
	}

	d.terminateSession()

	d.epoch = myEpoch
	d.session.try = 0
	d.emptyState = true

	d.routine()
}

func (d *Leecher) PeersNum() int {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return len(d.peers)
}

// UnregisterPeer removes a peer from the known list, preventing current or any future sessions with the peer
func (d *Leecher) UnregisterPeer(peer string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.session.peer == peer {
		d.terminateSession()
		d.routine()
	}
	delete(d.peers, peer)
	return nil
}

func (d *Leecher) terminate() {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.terminated = true
	close(d.quit)
	d.terminateSession()
}

// Stop interrupts the leecher, canceling all the pending operations.
// Stop waits until all the internal goroutines have finished.
func (d *Leecher) Stop() {
	d.terminate()
	d.wg.Wait()
}

func (d *Leecher) NotifyChunkReceived(sessionID uint32, last hash.Event, total essteam.Metric, done bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.session.agent == nil {
		return nil
	}
	if d.getSessionID() != sessionID {
		return nil
	}

	d.session.lastReceived = time.Now()
	if done {
		d.terminateSession()
		return nil
	}
	return d.session.agent.NotifyChunkReceived(last, total)
}
