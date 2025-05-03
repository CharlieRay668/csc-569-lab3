package main

import (
	"encoding/gob"
	"fmt"
	"lab3/shared"
	"math/rand"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

func init() {
	gob.Register(shared.Heartbeat{})
	gob.Register(shared.VoteRequest{})
	gob.Register(shared.VoteResponse{})
}

const (
	NewLeaderGrace = 2 * shared.MinElectonWait
)

var (
	selfID   int
	state    = shared.FOLLOWER
	term     = 0
	votedFor = -1

	lastHB time.Time

	votesMu sync.Mutex
	votes   int

	mu         sync.Mutex
	membership = shared.NewMembership()

	srv *rpc.Client
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("usage: go run client.go <id>")
		return
	}
	selfID, _ = strconv.Atoi(os.Args[1])
	rand.Seed(time.Now().UnixNano() + int64(selfID))

	var err error
	srv, err = rpc.DialHTTP("tcp", "localhost:9005")
	if err != nil {
		panic(err)
	}

	lastHB = time.Now()
	selfNode := shared.Node{ID: selfID, Alive: true, Time: float64(time.Now().Unix())}
	membership.Add(selfNode, &selfNode)
	fmt.Printf("Node %d → Follower (term %d)\n", selfID, term)

	// schedule ticks and elections
	scheduleHeartbeat()
	scheduleElection()

	select {}
}

func scheduleHeartbeat() {
	time.AfterFunc(shared.HearbeatInterval, func() {
		pullAndProcess()
		if state == shared.LEADER {
			broadcastHeartbeat()
		}
		scheduleHeartbeat()
	})
}

func scheduleElection() {
	delay := shared.MinElectonWait + time.Duration(rand.Intn(int(shared.MaxElectonWait-shared.MinElectonWait)))
	time.AfterFunc(delay, func() {
		// fmt.Printf("Node %d: election timeout (term %d) time since last HB: %v\n", selfID, term, time.Since(lastHB))
		if state == shared.LEADER || time.Since(lastHB) < NewLeaderGrace {
			scheduleElection()
			return
		}
		startElection()
		scheduleElection()
	})
}

func startElection() {
	mu.Lock()
	term++
	state = shared.CANDIDATE
	votedFor = selfID
	mu.Unlock()

	votesMu.Lock()
	votes = 1 // vote for self
	votesMu.Unlock()
	fmt.Printf("Node %d Candidate (term %d)\n", selfID, term)

	req := shared.VoteRequest{Term: term, CandidateID: selfID}
	msg := shared.Message{Type: 0, Msg: req}
	for peer := 1; peer <= shared.MaxNodes; peer++ {
		if peer == selfID {
			continue
		}
		send(peer, msg)
	}
	// count votes after election timeout
	time.AfterFunc(shared.MinElectonWait, countVotes)
}

func countVotes() {
	fmt.Printf("Node %d: counting votes (term %d)\n", selfID, term)
	votesMu.Lock()
	v := votes
	votesMu.Unlock()
	if state != shared.CANDIDATE {
		return
	}
	if v > shared.MaxNodes/2 {
		state = shared.LEADER
		fmt.Printf("Node %d Leader (term %d)\n", selfID, term)
	} else {
		state = shared.FOLLOWER
		fmt.Printf("Node %d Follower (term %d)\n", selfID, term)
	}
}

func pullAndProcess() {
	var inbox []shared.Message
	srv.Call("Reciever.GetMessages", selfID, &inbox)
	for _, m := range inbox {
		switch m.Type {
		case 2:
			hb := m.Msg.(shared.Heartbeat)
			if hb.Term < term {
				continue
			}
			term = hb.Term
			if state != shared.FOLLOWER {
				state = shared.FOLLOWER
				fmt.Printf("Node %d Follower (term %d)\n", selfID, term)
			}
			votedFor = -1
			membership = shared.CombineTables(membership, hb.Table)
			lastHB = time.Now()
		case 0:
			vr := m.Msg.(shared.VoteRequest)
			handleVoteRequest(vr)
		case 1:
			resp := m.Msg.(shared.VoteResponse)
			if state == shared.CANDIDATE && resp.Term == term && resp.VoteGranted {
				votesMu.Lock()
				votes++
				votesMu.Unlock()
			}
		}
	}
}

func handleVoteRequest(req shared.VoteRequest) {
	grant := false
	if req.Term > term {
		// Update to the higher term and reset state
		term = req.Term
		state = shared.FOLLOWER
		votedFor = -1
	}
	if req.Term == term && votedFor == -1 {
		// Grant vote if not already voted in this term
		grant = true
		votedFor = req.CandidateID
		lastHB = time.Now()
	}
	if grant {
		// Log the vote and switch to FOLLOWER
		fmt.Printf("Node %d Follower (term %d)\n", selfID, term)
		fmt.Printf("Node %d: voted for %d (term %d)\n", selfID, req.CandidateID, term)
	}
	// Send the vote response
	resp := shared.VoteResponse{Term: term, VoteGranted: grant}
	send(req.CandidateID, shared.Message{Type: 1, Msg: resp})
}

func broadcastHeartbeat() {
	hb := shared.Heartbeat{LeaderID: selfID, Term: term, Table: membership}
	msg := shared.Message{Type: 2, Msg: hb}
	for peer := 1; peer <= shared.MaxNodes; peer++ {
		if peer == selfID {
			continue
		}
		send(peer, msg)
	}
}

func send(dst int, m shared.Message) {
	var ok bool
	srv.Call("Reciever.AddMessage", &shared.MessageRequest{ID: dst, Msg: m}, &ok)
}
