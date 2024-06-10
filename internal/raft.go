package internal

import (
	"math/rand"
	"os"
	"sync"
	"time"
)

// -------------- STAGE 1: Implement the election timer --------------

const DebugCM = 1

type CMState int

const (
	Follower CMState = iota
	Candidate
	Leader
	Dead
)

func (s CMState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		panic("unreachable CMState type")
	}
}

type ConsensusModule struct {
	// mu handles race condition when concurrent occurs
	mu sync.Mutex

	// id is the server ID of this CM.
	id int

	// peerIds lists the IDs of our peers in the cluster.
	peerIds []int

	// server is the server containing this CM. It's used to issue RPC calls to peers.
	server *Server

	// Persistent Raft state on all servers
	currentTerm int

	// Volatile Raft state on all servers
	state              CMState
	electionResetEvent time.Time
}

// runElectionTimer implements an election timer. It should be launched whenever
// we want to start a timer towards becoming a candidate in a new election.
//
// This function is blocking and should be launched in a separate goroutine;
// it's designed to work for a single (one-shot) election timer, as it exits
// whenever the CM state changes from follower/candidate or the term changes.
func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()

	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()
	cm.dlog("election timer started (%v), term = %d", timeoutDuration, termStarted)

	// This loop until either:
	// - we discover the election timer is no longer needed, or
	// - the election timer expires and this CM becomes a candidate
	//
	// In a follower, this typically keeps running in the background for the
	// duration of the CM's lifetime.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		cm.mu.Lock()
		if cm.state != Follower && cm.state != Candidate {
			cm.dlog("in election timer state = %v, bailing out", cm.state)
			cm.mu.Unlock()
			return
		}

		if termStarted != cm.currentTerm {
			cm.dlog("in election timer term changed from %d to %d, bailing out", termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}

		// Start an election if we haven't heard from a leader or haven't voted for
		// someone for the duration of the timeout.
		if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeoutDuration {
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
}

// electionTimeout generates a pseudo-random election timeout duration.
func (cm *ConsensusModule) electionTimeout() time.Duration {
	// If RAFT_FORCE_MORE_REELECTION is set, stress-test by deliberately
	// generating a hard-coded number very often. This will create collisions
	// between different servers and force more re-elections.
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	}
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// -------------- STAGE 2: Becoming a candidate --------------
