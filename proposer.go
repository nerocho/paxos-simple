package paxos_simple

import (
	"sync"
	"sync/atomic"
	"time"
)

type Proposer struct {
	m sync.Mutex // lock

	id                   int         // proposer id
	acceptors            []string    // acceptors 地址
	proposal             float32     // 提案(id)
	currentValue         interface{} // 本轮提案值
	highestAcceptedID    float32     // 最高的acceptor id
	highestAcceptedValue interface{} // 最高的acceptor 的提案值
	consensusValue       interface{} // 共识的值
}

func (p *Proposer) close() {}

func (p *Proposer) clean() {
	p.proposal = 0
	p.currentValue = nil
	p.highestAcceptedID = 0
	p.highestAcceptedValue = nil
	p.consensusValue = nil
}

// 提议
func (p *Proposer) propose(value interface{}) interface{} {
	p.currentValue = value
	p.run()
	return nil
}

// 获取议员id
func (p *Proposer) getId() int {
	p.m.Lock()
	id := p.id
	p.m.Unlock()
	return id
}

// 计算选举人数
func (p *Proposer) getQuorumSize() int64 {
	p.m.Lock()
	size := int64(len(p.acceptors)/2 + 1) // 超过一半的人
	p.m.Unlock()
	return size
}

// 获取选举人
func (p *Proposer) getAcceptors() []string {
	p.m.Lock()
	acceptors := make([]string, len(p.acceptors))
	copy(acceptors, p.acceptors)
	p.m.Unlock()
	return acceptors
}

func (p *Proposer) run() {
	acceptors := p.getAcceptors()

	for p.consensusValue == nil {
		// phase 1 开始，该阶段可以理解为类似tcp的 "握手阶段"
		prepareMsgReq := p.prepare()
		var promiseSuccessNum int64
		var promiseFailureNum int64
		promiseSuccessChan := make(chan struct{})
		promiseFailureChan := make(chan struct{})

		logPrint("[proposer:%d] phase 1, prepareMsg:%v", p.getId(), prepareMsgReq)
		// 发起 "准备"请求
		for _, addr := range acceptors {
			go func(peerAddr string, prepareMsgReq PrepareMsg) {

				// 结算
				defer func() {
					// 超过一半及以上成功
					if atomic.LoadInt64(&promiseSuccessNum) >= p.getQuorumSize() {
						promiseSuccessChan <- struct{}{}
					}
					// 超过一半及以上失败
					if atomic.LoadInt64(&promiseFailureNum) >= p.getQuorumSize() {
						promiseFailureChan <- struct{}{}
						return
					}
				}()

				// 发送准备请求
				promiseMsgResp, err := p.sendPrepare(peerAddr, &prepareMsgReq)
				if err != nil || prepareMsgReq.Proposal != promiseMsgResp.Proposal || promiseMsgResp.AcceptorAddr != peerAddr {
					atomic.AddInt64(&promiseFailureNum, 1)
				}

				// 计数
				if promiseMsgResp.Success {
					atomic.AddInt64(&promiseSuccessNum, 1)
				} else {
					atomic.AddInt64(&promiseFailureNum, 1)
				}

				if promiseMsgResp.Proposal > 0 {
					p.setAccepted(promiseMsgResp)
				}

			}(addr, prepareMsgReq)
		}

		// phase 1 阶段结束
		select {
		case <-time.After(200 * time.Millisecond):
			continue
		case <-promiseFailureChan:
			continue
		case <-promiseSuccessChan:
		}

		// phase 2
		var acceptSuccessNum int64
		var acceptFailureNum int64
		acceptSuccessChan := make(chan struct{})
		acceptFailureChan := make(chan struct{})

		acceptMsgReq := p.accept()
		logPrint("[proposer:%d] phase 2, acceptMsg:%v", p.getId(), acceptMsgReq)

		for _, addr := range acceptors {
			go func(peerAddr string, acceptMsgReq AcceptMsg) {

				defer func() {
					if atomic.LoadInt64(&acceptSuccessNum) >= p.getQuorumSize() {
						acceptSuccessChan <- struct{}{}
						return
					}
					if atomic.LoadInt64(&acceptFailureNum) >= p.getQuorumSize() {
						acceptFailureChan <- struct{}{}
						return
					}
				}()

				// 发送"同意"请求
				acceptedMsgResp, err := p.sendAccept(peerAddr, &acceptMsgReq)
				if err != nil || acceptMsgReq.Proposal != acceptedMsgResp.Proposal || peerAddr != acceptedMsgResp.AcceptorAddr {
					atomic.AddInt64(&acceptFailureNum, 1)
					return
				}

				if acceptedMsgResp.Success {
					atomic.AddInt64(&acceptSuccessNum, 1)
				} else {
					atomic.AddInt64(&acceptFailureNum, 1)
				}

			}(addr, acceptMsgReq)
		}

		select {
		case <-time.After(200 * time.Millisecond):
			continue
		case <-acceptFailureChan:
			continue
		case <-acceptSuccessChan:
			p.consensusValue = acceptMsgReq.Value
			logPrint("[proposer:%d] reach consensus Value: %v", p.getId(), acceptMsgReq.Value)
			return
		}
	}
}

func (p *Proposer) prepare() PrepareMsg {
	p.m.Lock()
	proposal := generateNumber(p.id, p.proposal)
	msg := PrepareMsg{
		Proposal: proposal,
	}
	p.proposal = proposal
	p.m.Unlock()
	return msg
}

func (p *Proposer) accept() AcceptMsg {
	p.m.Lock()
	msg := AcceptMsg{
		Proposal: p.proposal,
		Value:    p.currentValue,
	}

	if p.highestAcceptedValue != nil {
		msg.Value = p.highestAcceptedValue
	}
	p.m.Unlock()
	return msg
}

func (p *Proposer) setAccepted(promiseMsgResp *PromiseMsg) {
	p.m.Lock()
	// 当前消息对应的提案超前于本地则更新
	if promiseMsgResp.Proposal > p.highestAcceptedID {
		p.highestAcceptedID = promiseMsgResp.AcceptedId
		p.highestAcceptedValue = promiseMsgResp.AcceptedValue
	}
	p.m.Unlock()
}

func (p *Proposer) sendPrepare(addr string, msg *PrepareMsg) (*PromiseMsg, error) {
	//reply := &PromiseMsg{}
	reply := new(PromiseMsg)
	err := callRpc(addr, "Acceptor", "ReceivePrepare", msg, reply)
	return reply, err
}

func (p *Proposer) sendAccept(addr string, msg *AcceptMsg) (*AcceptedMsg, error) {
	reply := new(AcceptedMsg)
	err := callRpc(addr, "Acceptor", "ReceiveAccept", msg, reply)
	return reply, err
}
