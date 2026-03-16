package peers

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/yggdrasil-network/yggstack/mod/activity"
)

// // // // // // // // // //

const (
	pollFast = 500 * time.Millisecond
	pollSlow = 5 * time.Second
)

// //

// MonitorObj опрашивает core.GetPeers() с адаптивной частотой.
type MonitorObj struct {
	core        CoreInterface
	callback    ChangeCallbackInterface
	connCounter *activity.CounterObj
	ctx         context.Context
	cancel      context.CancelFunc
	lastConn    atomic.Int64
	lastTotal   atomic.Int64
}

// NewMonitor создаёт новый монитор пиров.
func NewMonitor(core CoreInterface, callback ChangeCallbackInterface, connCounter *activity.CounterObj, ctx context.Context) *MonitorObj {
	ctx, cancel := context.WithCancel(ctx)
	return &MonitorObj{
		core:        core,
		callback:    callback,
		connCounter: connCounter,
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Cancel останавливает монитор.
func (m *MonitorObj) Cancel() {
	m.cancel()
}

func (m *MonitorObj) Run() {
	ticker := time.NewTicker(pollSlow)
	defer ticker.Stop()

	// Первоначальный снимок.
	m.Poll()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.Poll()

			if m.connCounter != nil && m.connCounter.Count() > 0 {
				ticker.Reset(pollFast)
			} else {
				ticker.Reset(pollSlow)
			}
		}
	}
}

func (m *MonitorObj) Poll() {
	if m.ctx.Err() != nil {
		return
	}
	peers := m.core.GetPeers()
	var connected, total int64
	for _, p := range peers {
		total++
		if p.Up {
			connected++
		}
	}
	if connected != m.lastConn.Load() || total != m.lastTotal.Load() {
		m.lastConn.Store(connected)
		m.lastTotal.Store(total)
		m.callback.OnPeerCountChanged(connected, total)
	}
}
