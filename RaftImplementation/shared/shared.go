package shared

import (
	"math/rand"
	"sync"
	"time"
)

func init() { rand.Seed(time.Now().UnixNano()) }

const (
	MaxNodes = 8

	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2

	// MinElectonWait   = 150 * time.Millisecond
	// MaxElectonWait   = 300 * time.Millisecond
	// HearbeatInterval = 50 * time.Millisecond
	MinElectonWait   = 3 * time.Second
	MaxElectonWait   = 5 * time.Second
	HearbeatInterval = 1 * time.Second
)

/* ---------------- basic node / membership (unchanged) -------------- */

type Node struct {
	ID        int
	Hbcounter int
	Time      float64
	Alive     bool
}

type Membership struct {
	mu      sync.RWMutex
	Members map[int]Node
}

func NewMembership() *Membership { return &Membership{Members: map[int]Node{}} }

func (m *Membership) Add(payload Node, reply *Node) error {
	m.mu.Lock()
	m.Members[payload.ID] = payload
	m.mu.Unlock()
	*reply = payload
	return nil
}

func (m *Membership) Update(payload Node, reply *Node) error {
	m.mu.Lock()
	if old, ok := m.Members[payload.ID]; ok {
		if payload.Hbcounter > old.Hbcounter {
			old.Hbcounter = payload.Hbcounter
		}
		if payload.Time > old.Time {
			old.Time = payload.Time
		}
		old.Alive = payload.Alive
		m.Members[payload.ID] = old
	}
	res := m.Members[payload.ID]
	m.mu.Unlock()
	*reply = res
	return nil
}

/* ---------------- fan‑out service ---------------- */

type VoteRequest struct {
	Term        int
	CandidateID int
}

type VoteResponse struct {
	Term        int
	VoteGranted bool
}

type Heartbeat struct {
	LeaderID int
	Term     int
	Table    *Membership
}

// Message.Type: 0 → VoteRequest, 1 → VoteResponse, 2 → Heartbeat

type Message struct {
	Type int
	Msg  any
}

type MessageRequest struct {
	ID  int // destination
	Msg Message
}

type Reciever struct { // (yes, the assignment’s original misspelling)
	mu    sync.Mutex
	Inbox map[int][]Message // dest‑ID → queue
}

func NewReciever() *Reciever { return &Reciever{Inbox: map[int][]Message{}} }

func (r *Reciever) AddMessage(payload *MessageRequest, reply *bool) error {
	r.mu.Lock()
	r.Inbox[payload.ID] = append(r.Inbox[payload.ID], payload.Msg)
	r.mu.Unlock()
	*reply = true
	return nil
}

func (r *Reciever) GetMessages(id int, out *[]Message) error {
	r.mu.Lock()
	*out = append((*out)[:0], r.Inbox[id]...)
	delete(r.Inbox, id)
	r.mu.Unlock()
	return nil
}

/* ---------------- helper to merge membership tables -------------- */

func CombineTables(a, b *Membership) *Membership {
	out := NewMembership()
	a.mu.RLock()
	for id, n := range a.Members {
		out.Members[id] = n
	}
	a.mu.RUnlock()
	b.mu.RLock()
	for id, nb := range b.Members {
		if na, ok := out.Members[id]; ok {
			if nb.Hbcounter > na.Hbcounter {
				na.Hbcounter = nb.Hbcounter
				na.Time = nb.Time
			}
			if nb.Time > na.Time {
				na.Time = nb.Time
			}
			na.Alive = nb.Alive
			out.Members[id] = na
		} else {
			out.Members[id] = nb
		}
	}
	b.mu.RUnlock()
	return out
}
