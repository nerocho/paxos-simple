package paxos_simple

import (
	"fmt"
	"testing"
	"time"
)

// 初始化集群
func makeCluster(proposerNum, acceptorNum, learnNum int) (p []*Proposer, a []*Acceptor, l []*Learner) {
	acceptorPort := 4100
	learnerPort := 4200

	learners := make([]string, learnNum)
	for i := 0; i < learnNum; i++ {
		learnerPort++
		learner := &Learner{localAddr: formatAddr(learnerPort)}
		l = append(l, learner)
		learners[i] = learner.localAddr
		l[i].startRpc()
	}

	acceptors := make([]string, acceptorNum)
	for i := 0; i < acceptorNum; i++ {
		acceptorPort++
		acceptor := &Acceptor{
			localAddr: formatAddr(acceptorPort),
			learners:  learners,
		}
		a = append(a, acceptor)
		acceptors[i] = acceptor.localAddr
		a[i].startRpc()
	}

	for i := 0; i < proposerNum; i++ {
		proposer := &Proposer{
			acceptors: acceptors,
			id:        i + 1,
		}
		p = append(p, proposer)
	}
	return
}

func formatAddr(port int) string {
	return fmt.Sprintf("127.0.0.1:%d", port)
}

// 清除数据
func clean(p []*Proposer, a []*Acceptor, l []*Learner) {
	for _, i := range p {
		i.clean()
	}
	for _, i := range a {
		i.clean()
	}
	for _, i := range l {
		i.clean()
	}
}

// 关闭端口
func close(p []*Proposer, a []*Acceptor, l []*Learner) {
	for _, i := range p {
		i.close()
	}
	for _, i := range a {
		i.close()
	}
	for _, i := range l {
		i.close()
	}
}

func checkOne(t *testing.T, p []*Proposer, value interface{}) {
	for _, i := range p {
		if i.consensusValue != value {
			t.Fatalf("错误的共识值，期望值：%v，收到值：%v", value, i.consensusValue)
		}
	}
}

func checkMulti(t *testing.T, p []*Proposer) {
	var value interface{}
	for _, i := range p {
		if i.consensusValue == nil {
			t.Fatalf("错误的共识值, 共识值为: nil")
		}
		if value != nil && value != i.consensusValue {
			t.Fatalf("错误的共识值, 前共识值:%v 当前共识值:%v", value, i.consensusValue)
		}
		if value == nil {
			value = i.consensusValue
		}
	}
	if value == nil {
		t.Fatalf("无共识值存在")
	}
}

func TestBasicPaxos(t *testing.T) {
	pNum := 1
	aNum := 3
	lNum := 2
	logPrint("单Proposer测试 proposer :%d, acceptor :%d, learner :%d 开始", pNum, aNum, lNum)
	p, a, l := makeCluster(pNum, aNum, lNum)
	defer close(p, a, l)
	clean(p, a, l)

	p[0].propose(100)
	time.Sleep(1 * time.Second)
	checkOne(t, p, 100)
	clean(p, a, l)
	logPrint("单Proposer测试 proposer :%d, acceptor :%d, learner :%d 结束", pNum, aNum, lNum)
}

func TestMultiProposer(t *testing.T) {
	pNum := 3
	aNum := 3
	lNum := 2
	logPrint("多Proposer测试 proposer :%d, acceptor :%d, learner :%d 开始", pNum, aNum, lNum)
	p, a, l := makeCluster(pNum, aNum, lNum)
	defer close(p, a, l)

	for i, proposer := range p {
		proposer.propose(100 + i)
	}
	time.Sleep(1 * time.Second)
	checkMulti(t, p)
	clean(p, a, l)

	for i, proposer := range p {
		proposer.propose(200 + i)
	}
	time.Sleep(1 * time.Second)
	checkMulti(t, p)
	clean(p, a, l)

	for i, proposer := range p {
		proposer.propose(300 + i)
	}
	time.Sleep(1 * time.Second)
	checkMulti(t, p)
	clean(p, a, l)
	logPrint("多Proposer测试 proposer :%d, acceptor :%d, learner :%d 结束", pNum, aNum, lNum)
}

func TestMultiProposerUnreliable(t *testing.T) {
	pNum := 3
	aNum := 3
	lNum := 2
	logPrint("多Proposer+acceptor模拟不可靠测试 proposer :%d, acceptor :%d, learner :%d 开始", pNum, aNum, lNum)
	p, a, l := makeCluster(pNum, aNum, lNum)
	for _, acceptor := range a {
		acceptor.isUnreliable = true
	}
	defer close(p, a, l)

	for i, proposer := range p {
		proposer.propose(100 + i)
	}
	time.Sleep(1 * time.Second)
	checkMulti(t, p)
	clean(p, a, l)

	for i, proposer := range p {
		proposer.propose(200 + i)
	}
	time.Sleep(1 * time.Second)
	checkMulti(t, p)
	clean(p, a, l)

	for i, proposer := range p {
		proposer.propose(300 + i)
	}
	time.Sleep(1 * time.Second)
	checkMulti(t, p)
	clean(p, a, l)
	logPrint("多Proposer+acceptor模拟不可靠测试 proposer :%d, acceptor :%d, learner :%d 结束", pNum, aNum, lNum)
}
