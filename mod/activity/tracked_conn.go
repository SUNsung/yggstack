package activity

import (
	"net"
	"strconv"
	"sync"
	"sync/atomic"
)

// // // // // // // // // //

// TrackedConnObj оборачивает net.Conn и уведомляет callback при закрытии.
type TrackedConnObj struct {
	net.Conn
	ConnId   string
	Callback CallbackInterface
	Counter  *CounterObj
	closed   sync.Once
}

func (t *TrackedConnObj) Close() error {
	var innerErr error
	t.closed.Do(func() {
		// Decrement откладывается, чтобы выполниться даже при панике в OnConnectionClosed.
		defer t.Counter.Decrement()
		t.Callback.OnConnectionClosed(t.ConnId)
		innerErr = t.Conn.Close()
	})
	return innerErr
}

// //

var idCounter atomic.Uint64

// GenerateConnId создаёт уникальный идентификатор соединения через атомарный счётчик.
func GenerateConnId(prefix string, addr string) string {
	id := idCounter.Add(1)
	buf := make([]byte, 0, len(prefix)+1+len(addr)+1+20)
	buf = append(buf, prefix...)
	buf = append(buf, '-')
	buf = append(buf, addr...)
	buf = append(buf, '-')
	buf = strconv.AppendUint(buf, id, 10)
	return string(buf)
}
