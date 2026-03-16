package mapping

import (
	"context"
	"net"
	"sync"
	"sync/atomic"

	"github.com/yggdrasil-network/yggstack/mod/activity"
)

// // // // // // // // // //

// UDPSessionObj хранит состояние одной UDP-сессии.
type UDPSessionObj struct {
	Conn         net.Conn
	RemoteAddr   net.Addr
	LastActivity atomic.Int64
	ConnId       string                     // "" если callback равен nil
	Callback     activity.CallbackInterface // nil если отслеживание отключено
	Counter      *activity.CounterObj       // nil если отслеживание отключено
	CloseOnce    sync.Once
	cancel       context.CancelFunc // отменяет контекст сессии, останавливает ReverseProxyUDP
}

// UDPSessionMapObj — типизированная конкурентная карта UDP-сессий.
type UDPSessionMapObj struct {
	mu   sync.RWMutex
	data map[string]*UDPSessionObj
}

// NewUDPSessionMap создаёт пустую карту сессий.
func NewUDPSessionMap() *UDPSessionMapObj {
	return &UDPSessionMapObj{data: make(map[string]*UDPSessionObj)}
}
