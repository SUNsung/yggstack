package core

import (
	"context"
	"crypto/ed25519"
	"net"
	"time"

	golog "github.com/gologme/log"
)

// // // // // // // // // //

// Interface — публичный контракт узла Yggdrasil
type Interface interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
	Listen(network, address string) (net.Listener, error)
	ListenPacket(network, address string) (net.PacketConn, error)
	Address() net.IP
	Subnet() net.IPNet
	PublicKey() ed25519.PublicKey
	MTU() uint64
	AddPeer(uri string) error
	RemovePeer(uri string) error
	GetPeers() []PeerInfoObj
	EnableMulticast(logger *golog.Logger) error
	DisableMulticast() error
	EnableAdmin(addr string) error
	DisableAdmin() error
	Close() error
}

// PeerInfoObj — информация о пире
type PeerInfoObj struct {
	URI     string
	Up      bool
	Inbound bool
	Key     ed25519.PublicKey
	Latency time.Duration
	Cost    uint64
	RXBytes uint64
	TXBytes uint64
	Uptime  time.Duration

	// Последняя ошибка подключения; nil если нет
	LastError     error
	LastErrorTime time.Time
}
