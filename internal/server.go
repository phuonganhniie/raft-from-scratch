package internal

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
)

// Server container for a Raft Consensus Module. Exposes Raft to the network
// and enables RPCs between Raft peers.
//
// Server wraps a raft.ConsensusModule along with a rpc.Server that exposes its
// methods as RPC endpoints. It also manages the peers of the Raft server. The
// main goal of this type is to simplify the code of raft.Server for
// presentation purposes. raft.ConsensusModule has a *Server to do its peer
// communication and doesn't have to worry about the specifics of running an
// RPC server.
type Server struct {
	mu sync.Mutex

	serverId int
	peerId   []int

	cm *ConsensusModule

	rpcServer *rpc.Server
	listener  net.Listener

	peerClients map[int]*rpc.Client
}

func (s *Server) CallRPC(id int, serviceMethod string, arg interface{}, reply interface{}) error {
	s.mu.Lock()
	peerClient := s.peerClients[id]
	s.mu.Unlock()

	// If this is called after shutdown (where client.Close is called), it will
	// return an error.
	if peerClient == nil {
		return fmt.Errorf("call client %d after it's closed", id)
	}
	return peerClient.Call(serviceMethod, arg, reply)
}
