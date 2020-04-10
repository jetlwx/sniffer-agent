package session_dealer

import "github.com/jetlwx/sniffer-agent/model"

type ConnSession interface {
	ReceiveTCPPacket(*model.TCPPacket)
	Close()
}
