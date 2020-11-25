package streamseeder

import (
	"bytes"
	"errors"
	"sync"

	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/ethereum/go-ethereum/common"

	"github.com/Fantom-foundation/go-opera/gossip/essteam"
)

type Seeder struct {
	callback Callbacks

	peerSessions map[string][]uint32
	sessions     map[sessionIDAndPeer]sessionState

	notifyUnregisteredPeer chan string
	notifyReceivedRequest  chan *requestAndPeer
	quit                   chan struct{}

	parallelTasks chan func()

	cfg Config

	wg sync.WaitGroup
}

func New(cfg Config, callbacks Callbacks) *Seeder {
	return &Seeder{
		callback:               callbacks,
		peerSessions:           make(map[string][]uint32),
		sessions:               make(map[sessionIDAndPeer]sessionState),
		notifyUnregisteredPeer: make(chan string, 128),
		notifyReceivedRequest:  make(chan *requestAndPeer, 16),
		parallelTasks:          make(chan func(), cfg.SenderThreads*2),
		quit:                   make(chan struct{}),
		cfg:                    cfg,
	}
}

type Callbacks struct {
	ForEachEvent func(start []byte, onEvent func(key hash.Event, event interface{}, size uint64) bool)
}

type Peer struct {
	ID           string
	SendChunk    func(essteam.Response, hash.Events) error
	Misbehaviour func(error)
}

type sessionIDAndPeer struct {
	id   uint32
	peer string
}

type requestAndPeer struct {
	request essteam.Request
	peer    Peer
}

type sessionState struct {
	origSelector []byte
	next         []byte
	stop         []byte
	done         bool
	sendChunk    func(essteam.Response, hash.Events) error
}

func (s *Seeder) Start() {
	for i := 0; i < s.cfg.SenderThreads; i++ {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			worker(s.parallelTasks, s.quit)
		}()
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		go s.loop()
	}()
}

// Stop interrupts the seeder, canceling all the pending operations.
// Stop waits until all the internal goroutines have finished.
func (s *Seeder) Stop() {
	close(s.quit)
	s.wg.Wait()
}

var (
	ErrWrongType        = errors.New("wrong request type")
	ErrWrongSelectorLen = errors.New("wrong event selector length")
	ErrSelectorMismatch = errors.New("session selector mismatch")
	errTerminated       = errors.New("terminated")
)

func (s *Seeder) NotifyRequestReceived(peer Peer, r essteam.Request) (err error, peerErr error) {
	if len(r.Session.Start) > len(hash.ZeroEvent) {
		return nil, ErrWrongSelectorLen
	}
	if r.Type != essteam.RequestIDs && r.Type != essteam.RequestEvents {
		return nil, ErrWrongType
	}
	op := &requestAndPeer{
		peer:    peer,
		request: r,
	}
	select {
	case s.notifyReceivedRequest <- op:
		return nil, nil
	case <-s.quit:
		return errTerminated, nil
	}
}

func (s *Seeder) UnregisterPeer(peer string) error {
	select {
	case s.notifyUnregisteredPeer <- peer:
		return nil
	case <-s.quit:
		return errTerminated
	}
}

func (s *Seeder) loop() {
	for {
		// Wait for an outside event to occur
		select {
		case <-s.quit:
			// terminating, abort all operations
			return

		case peerID := <-s.notifyUnregisteredPeer:
			sessions := s.peerSessions[peerID]
			for _, sid := range sessions {
				delete(s.sessions, sessionIDAndPeer{sid, peerID})
			}
			delete(s.peerSessions, peerID)

		case op := <-s.notifyReceivedRequest:
			// prune oldest session
			sessions := s.peerSessions[op.peer.ID]
			if len(sessions) > 2 {
				oldest := sessions[0]
				sessions = sessions[1:]
				delete(s.sessions, sessionIDAndPeer{oldest, op.peer.ID})
			}

			// add session
			session, ok := s.sessions[sessionIDAndPeer{op.request.Session.ID, op.peer.ID}]
			if !ok {
				session.origSelector = op.request.Session.Start
				session.next = op.request.Session.Start
				session.stop = op.request.Session.Stop
				session.sendChunk = op.peer.SendChunk
				sessions = append(sessions, op.request.Session.ID)
				s.peerSessions[op.peer.ID] = sessions
			}

			// sanity check (cannot change session parameters after it's created)
			if bytes.Compare(session.origSelector, op.request.Session.Start) != 0 {
				op.peer.Misbehaviour(ErrSelectorMismatch)
				continue
			}

			if session.done {
				continue
			}

			allConsumed := true
			resp := essteam.Response{}
			size := uint64(0)
			var last hash.Event
			var ids hash.Events
			s.callback.ForEachEvent(session.next, func(id hash.Event, event interface{}, eventSize uint64) bool {
				if bytes.Compare(id.Bytes(), session.stop) >= 0 {
					return false
				}
				lim := op.request.Limit
				limitReached := idx.Event(len(resp.IDs)) >= lim.Num || idx.Event(len(resp.Events)) >= lim.Num || size >= lim.Size
				if size != 0 && limitReached {
					allConsumed = false
					return false
				}
				if op.request.Type == essteam.RequestIDs {
					resp.Events = append(resp.Events, event)
					ids = append(ids, id)
					size += eventSize
				} else {
					resp.IDs = append(resp.IDs, id)
					ids = resp.IDs
					size += uint64(len(id))
				}
				last = id
				return true
			})
			// update session
			nextBn := last.Big()
			nextBn.Add(nextBn, common.Big1)
			session.next = common.BytesToHash(nextBn.Bytes()).Bytes()
			session.done = allConsumed
			s.sessions[sessionIDAndPeer{op.request.Session.ID, op.peer.ID}] = session

			resp.Done = allConsumed
			resp.SessionID = op.request.Session.ID

			s.parallelTasks <- func() {
				_ = session.sendChunk(resp, ids)
			}
		}
	}
}

func worker(tasksC <-chan func(), quit <-chan struct{}) {
	for {
		select {
		case <-quit:
			return
		case job := <-tasksC:
			job()
		}
	}
}
