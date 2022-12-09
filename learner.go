package paxos_simple

import (
	"log"
	"net"
	"net/rpc"
	"sync"
)

type Learner struct {
	m sync.Mutex // lock

	localAddr      string                  // 本地地址
	acceptorCount  map[float32]int         //
	acceptedValue  map[float32]interface{} //
	consensusValue interface{}             // 共识值
	quorumSize     int                     //
	listener       net.Listener            //
}

func (l *Learner) getQuorumSize() int {
	l.m.Lock()
	size := l.quorumSize
	l.m.Unlock()
	return size
}

func (l *Learner) ReceiveAccepted(arg *AcceptedMsg, reply *EmptyMsg) error {
	// PASS
	return nil
}

func (l *Learner) startRpc() {
	rpcx := rpc.NewServer()
	err := rpcx.Register(l)
	if err != nil {
		return
	}
	lis, e := net.Listen("tcp", l.localAddr)
	l.listener = lis
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			go rpcx.ServeConn(conn)
		}
	}()
}

func (le *Learner) clean() {
	le.acceptedValue = nil
}

func (le *Learner) close() {
	_ = le.listener.Close()
}
