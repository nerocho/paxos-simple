package paxos_simple

import (
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
)

type Acceptor struct {
	m sync.Mutex // lock

	localAddr     string       // 本地tcp地址
	learners      []string     // learner 地址
	proposal      float32      // 最高的 提案id
	acceptedId    float32      // 接收的 提案id
	acceptedValue interface{}  // 接收的 提案值
	listener      net.Listener //
	isUnreliable  bool         // 模拟不可靠网络
}

func (a *Acceptor) getLearners() []string {
	a.m.Lock()
	learners := make([]string, len(a.learners))
	copy(learners, a.learners)
	a.m.Unlock()
	return learners
}

func (a *Acceptor) getAddr() string {
	a.m.Lock()
	addr := a.localAddr
	a.m.Unlock()
	return addr
}

func (a *Acceptor) ReceivePrepare(arg *PrepareMsg, reply *PromiseMsg) error {
	logPrint("[acceptor] %s ReceivePrepare:%v", a.localAddr, arg)

	reply.Proposal = arg.Proposal
	reply.AcceptorAddr = a.getAddr()

	if arg.Proposal > a.proposal {
		a.proposal = arg.Proposal
		reply.Success = true
		if a.acceptedId > 0 && a.acceptedValue != nil {
			reply.AcceptedId = a.acceptedId
			reply.AcceptedValue = a.acceptedValue
		}
	}

	return nil
}

func (a *Acceptor) ReceiveAccept(arg *AcceptMsg, reply *AcceptedMsg) error {
	logPrint("[acceptor %s ReceiveAccept:%v ]", a.localAddr, arg)

	reply.Proposal = arg.Proposal

	if arg.Proposal == a.proposal {
		reply.Success = true
		reply.AcceptorAddr = a.getAddr()

		a.proposal = arg.Proposal
		a.acceptedId = arg.Proposal
		a.acceptedValue = arg.Value
		for _, learner := range a.getLearners() {
			_ = callRpc(learner, "Learner", "ReceiveAccepted", reply, EmptyMsg{})
		}
	}
	return nil
}

func (a *Acceptor) startRpc() {
	rpcx := rpc.NewServer()
	err := rpcx.Register(a)
	if err != nil {
		return
	}
	l, e := net.Listen("tcp", a.localAddr)
	a.listener = l
	if e != nil {
		log.Fatal("listen error: ", e)
	}

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				continue
			}
			if a.isUnreliable && rand.Int63()*1000 < 300 {
				_ = conn.Close()
				continue
			}
			go rpcx.ServeConn(conn)
		}
	}()
}
func (a *Acceptor) clean() {
	a.proposal = 0
	a.acceptedId = 0
	a.acceptedValue = nil
}

func (a *Acceptor) close() {
	_ = a.listener.Close()
}
