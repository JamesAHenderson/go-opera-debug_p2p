package inter

import (
	"crypto/sha256"

	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
)

type EventI interface {
	dag.Event
	Version() uint8
	CreationTime() Timestamp
	MedianTime() Timestamp
	PrevEpochHash() *hash.Hash
	Extra() []byte
	GasPowerLeft() GasPowerLeft
	GasPowerUsed() uint64

	HashToSign() hash.Hash
	Locator() EventLocator

	// Payload-related fields

	AnyTxs() bool
	AnyBlockVotes() bool
	AnyEpochVote() bool
	AnyMisbehaviourProofs() bool
	PayloadHash() hash.Hash
}

type EventLocator struct {
	BaseHash    hash.Hash
	Epoch       idx.Epoch
	Seq         idx.Event
	Lamport     idx.Lamport
	Creator     idx.ValidatorID
	PayloadHash hash.Hash
}

type SignedEventLocator struct {
	EventLocator
	Sig Signature
}

func AsSignedEventLocator(e EventPayloadI) SignedEventLocator {
	return SignedEventLocator{
		EventLocator: e.Locator(),
		Sig:          e.Sig(),
	}
}

type EventPayloadI interface {
	EventI
	Sig() Signature

	Txs() types.Transactions
	EpochVote() LlrEpochVote
	BlockVotes() LlrBlockVotes
	MisbehaviourProofs() []MisbehaviourProof
}

var emptyPayloadHash1 = CalcPayloadHash(&MutableEventPayload{extEventData: extEventData{version: 1}})

func EmptyPayloadHash(version uint8) hash.Hash {
	if version == 0 {
		return hash.Hash(types.EmptyRootHash)
	} else {
		return emptyPayloadHash1
	}
}

type baseEvent struct {
	dag.BaseEvent
}

type mutableBaseEvent struct {
	dag.MutableBaseEvent
}

type extEventData struct {
	version       uint8
	creationTime  Timestamp
	medianTime    Timestamp
	prevEpochHash *hash.Hash
	gasPowerLeft  GasPowerLeft
	gasPowerUsed  uint64
	extra         []byte

	anyTxs                bool
	anyBlockVotes         bool
	anyEpochVote          bool
	anyMisbehaviourProofs bool
	payloadHash           hash.Hash
}

type sigData struct {
	sig Signature
}

type payloadData struct {
	txs                types.Transactions
	misbehaviourProofs []MisbehaviourProof

	epochVotes LlrEpochVote
	blockVotes LlrBlockVotes
}

type Event struct {
	baseEvent
	extEventData

	// cache
	_baseHash    *hash.Hash
	_locatorHash *hash.Hash
}

type SignedEvent struct {
	Event
	sigData
}

type EventPayload struct {
	SignedEvent
	payloadData

	// cache
	_size int
}

type MutableEventPayload struct {
	mutableBaseEvent
	extEventData
	sigData
	payloadData
}

func (e *Event) HashToSign() hash.Hash {
	return *e._locatorHash
}

func (e *Event) Locator() EventLocator {
	return EventLocator{
		BaseHash:    *e._baseHash,
		Epoch:       e.Epoch(),
		Seq:         e.Seq(),
		Lamport:     e.Lamport(),
		Creator:     e.Creator(),
		PayloadHash: e.PayloadHash(),
	}
}

func (e *EventPayload) Size() int {
	return e._size
}

func (e *extEventData) Version() uint8 { return e.version }

func (e *extEventData) CreationTime() Timestamp { return e.creationTime }

func (e *extEventData) MedianTime() Timestamp { return e.medianTime }

func (e *extEventData) PrevEpochHash() *hash.Hash { return e.prevEpochHash }

func (e *extEventData) Extra() []byte { return e.extra }

func (e *extEventData) PayloadHash() hash.Hash { return e.payloadHash }

func (e *extEventData) AnyTxs() bool { return e.anyTxs }

func (e *extEventData) AnyMisbehaviourProofs() bool { return e.anyMisbehaviourProofs }

func (e *extEventData) AnyEpochVote() bool { return e.anyEpochVote }

func (e *extEventData) AnyBlockVotes() bool { return e.anyBlockVotes }

func (e *extEventData) GasPowerLeft() GasPowerLeft { return e.gasPowerLeft }

func (e *extEventData) GasPowerUsed() uint64 { return e.gasPowerUsed }

func (e *sigData) Sig() Signature { return e.sig }

func (e *payloadData) Txs() types.Transactions { return e.txs }

func (e *payloadData) MisbehaviourProofs() []MisbehaviourProof { return e.misbehaviourProofs }

func (e *payloadData) BlockVotes() LlrBlockVotes { return e.blockVotes }

func (e *payloadData) EpochVote() LlrEpochVote { return e.epochVotes }

func CalcTxHash(txs types.Transactions) hash.Hash {
	return hash.Hash(types.DeriveSha(txs, new(trie.Trie)))
}

func CalcReceiptsHash(receipts []*types.ReceiptForStorage) hash.Hash {
	hasher := sha256.New()
	_ = rlp.Encode(hasher, receipts)
	return hash.BytesToHash(hasher.Sum(nil))
}

func CalcMisbehaviourProofsHash(mps []MisbehaviourProof) hash.Hash {
	hasher := sha256.New()
	_ = rlp.Encode(hasher, mps)
	return hash.BytesToHash(hasher.Sum(nil))
}

func CalcPayloadHash(e EventPayloadI) hash.Hash {
	if e.Version() == 0 {
		return CalcTxHash(e.Txs())
	}
	return hash.Of(hash.Of(CalcTxHash(e.Txs()).Bytes(), CalcMisbehaviourProofsHash(e.MisbehaviourProofs()).Bytes()).Bytes(), hash.Of(e.EpochVote().Hash().Bytes(), e.BlockVotes().Hash().Bytes()).Bytes())
}

func (e *MutableEventPayload) SetVersion(v uint8) { e.version = v }

func (e *MutableEventPayload) SetCreationTime(v Timestamp) { e.creationTime = v }

func (e *MutableEventPayload) SetMedianTime(v Timestamp) { e.medianTime = v }

func (e *MutableEventPayload) SetPrevEpochHash(v *hash.Hash) { e.prevEpochHash = v }

func (e *MutableEventPayload) SetExtra(v []byte) { e.extra = v }

func (e *MutableEventPayload) SetPayloadHash(v hash.Hash) { e.payloadHash = v }

func (e *MutableEventPayload) SetGasPowerLeft(v GasPowerLeft) { e.gasPowerLeft = v }

func (e *MutableEventPayload) SetGasPowerUsed(v uint64) { e.gasPowerUsed = v }

func (e *MutableEventPayload) SetSig(v Signature) { e.sig = v }

func (e *MutableEventPayload) SetTxs(v types.Transactions) {
	e.txs = v
	e.anyTxs = len(v) != 0
}

func (e *MutableEventPayload) SetMisbehaviourProofs(v []MisbehaviourProof) {
	e.misbehaviourProofs = v
	e.anyMisbehaviourProofs = len(v) != 0
}

func (e *MutableEventPayload) SetBlockVotes(v LlrBlockVotes) {
	e.blockVotes = v
	e.anyBlockVotes = len(v.Votes) != 0
}

func (e *MutableEventPayload) SetEpochVote(v LlrEpochVote) {
	e.epochVotes = v
	e.anyEpochVote = v.Epoch != 0 && v.Vote != hash.Zero
}

func calcEventID(h hash.Hash) (id [24]byte) {
	copy(id[:], h[:24])
	return id
}

func calcEventHashes(ser []byte, e EventI) (locator hash.Hash, base hash.Hash) {
	base = hash.Of(ser)
	if e.Version() < 1 {
		return base, base
	}
	locator = hash.Of(base.Bytes(), e.Epoch().Bytes(), e.Seq().Bytes(), e.Lamport().Bytes(), e.Creator().Bytes(), e.PayloadHash().Bytes())
	return locator, base
}

func (e *MutableEventPayload) calcHashes() (locator hash.Hash, base hash.Hash) {
	b, _ := e.immutable().Event.MarshalBinary()
	return calcEventHashes(b, e)
}

func (e *MutableEventPayload) size() int {
	b, err := e.immutable().MarshalBinary()
	if err != nil {
		panic("can't encode: " + err.Error())
	}
	return len(b)
}

func (e *MutableEventPayload) HashToSign() hash.Hash {
	h, _ := e.calcHashes()
	return h
}

func (e *MutableEventPayload) Locator() EventLocator {
	_, baseHash := e.calcHashes()
	return EventLocator{
		BaseHash:    baseHash,
		Epoch:       e.Epoch(),
		Seq:         e.Seq(),
		Lamport:     e.Lamport(),
		Creator:     e.Creator(),
		PayloadHash: e.PayloadHash(),
	}
}

func (e *MutableEventPayload) Size() int {
	return e.size()
}

func (me *MutableEventPayload) build(locatorHash hash.Hash, baseHash hash.Hash, size int) *EventPayload {
	return &EventPayload{
		SignedEvent: SignedEvent{
			Event: Event{
				baseEvent:    baseEvent{*me.MutableBaseEvent.Build(calcEventID(locatorHash))},
				extEventData: me.extEventData,
				_baseHash:    &baseHash,
				_locatorHash: &locatorHash,
			},
			sigData: me.sigData,
		},
		payloadData: me.payloadData,
		_size:       size,
	}
}

func (me *MutableEventPayload) immutable() *EventPayload {
	return me.build(hash.Hash{}, hash.Hash{}, 0)
}

func (e *MutableEventPayload) Build() *EventPayload {
	locatorHash, baseHash := e.calcHashes()
	payloadSer, _ := e.immutable().MarshalBinary()
	return e.build(locatorHash, baseHash, len(payloadSer))
}

func (l EventLocator) HashToSign() hash.Hash {
	return hash.Of(l.BaseHash.Bytes(), l.Epoch.Bytes(), l.Seq.Bytes(), l.Lamport.Bytes(), l.Creator.Bytes(), l.PayloadHash.Bytes())
}

func (l EventLocator) ID() hash.Event {
	h := l.HashToSign()
	copy(h[0:4], l.Epoch.Bytes())
	copy(h[4:8], l.Lamport.Bytes())
	return hash.Event(h)
}
