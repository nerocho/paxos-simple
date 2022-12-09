package paxos_simple

type PrepareMsg struct {
	Proposal float32
}

type PromiseMsg struct {
	AcceptorAddr  string
	Proposal      float32
	Success       bool
	AcceptedId    float32
	AcceptedValue interface{}
}

type AcceptMsg struct {
	Proposal     float32
	AcceptorAddr string
	Value        interface{}
}

type AcceptedMsg struct {
	Proposal     float32
	AcceptorAddr string
	Success      bool
}

type EmptyMsg struct{}
