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

type LogEntry struct {
	Command interface{}
	Term    int
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
	votedFor    int
	log         []LogEntry

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

type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // index of candidate's last log entry
}

type RequestVoteReply struct {
	Term        int  // current term, for candidate to update itself
	VoteGranted bool // true means candidate receives vote
}

// startElection starts new election with this CM as a candidate after election timer timeout.
// Expect cm.mu to be locked.
func (cm *ConsensusModule) startElection() {
	cm.state = Candidate
	cm.currentTerm++ // increase current term
	savedCurrentTerm := cm.currentTerm
	cm.electionResetEvent = time.Now() // reset election timer
	cm.dlog("election reset event at: %v", cm.electionResetEvent)
	cm.votedFor = cm.id
	cm.dlog("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)

	votesReceived := 1 // vote for self

	// send RequestVote RPCs to all other servers
	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			requestVote := RequestVoteArgs{
				Term:        savedCurrentTerm,
				CandidateId: cm.id,
			}

			var reply RequestVoteReply
			cm.dlog("sending RequestVote to %d: %+v", peerId, requestVote)

			err := cm.server.CallRPC(peerId, "ConsensusModule.RequestVote", requestVote, reply)
			if err != nil {
				return
			}

			cm.mu.Lock()
			defer cm.mu.Unlock()
			cm.dlog("received RequestVote %+v", reply)

			if cm.state != Candidate {
				cm.dlog("while waiting for reply, state = %v", cm.state)
				return
			}

			// set CM state to follower if reply term gt saved term in this goroutine
			if reply.Term > savedCurrentTerm {
				cm.dlog("term out of date in RequestVoteReply")
				cm.becomeFollower(reply.Term)
				return
			}

			if reply.Term == savedCurrentTerm {
				if reply.VoteGranted {
					votesReceived++
					if votesReceived*2 > len(cm.peerIds)+1 {
						// won the election
						cm.dlog("win the election with %d votes", votesReceived)
						cm.becomeLeader()
						return
					}
				}
			}
		}(peerId)
	}

	// Run another election timer, in case this election not successful
	go cm.runElectionTimer()
}
