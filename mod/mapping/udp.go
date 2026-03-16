package mapping

import (
	"context"
	"net"
	"time"

	"github.com/yggdrasil-network/yggdrasil-go/src/core"

	"github.com/yggdrasil-network/yggstack/mod/activity"
	"github.com/yggdrasil-network/yggstack/src/types"
)

// // // // // // // // // //

// StartLocalUDP перенаправляет локальный UDP-порт на удалённый адрес Yggdrasil.
func StartLocalUDP(node NodeInterface, mappings []types.UDPMapping, sessionTimeout time.Duration) {
	log := node.GetLogger()
	ns := node.GetNetstack()
	wg := node.GetComponentsWg()

	for _, mapping := range mappings {
		wg.Add(1)
		go func(m types.UDPMapping) {
			defer wg.Done()
			conn, err := net.ListenUDP("udp", m.Listen)
			if err != nil {
				log.Errorf("Failed to listen on local UDP %s: %s", m.Listen, err)
				return
			}
			node.AddCloser(conn)
			log.Infof("Mapping local UDP port %d to Yggdrasil %s", m.Listen.Port, m.Mapped)
			runUDPLoop(node, sessionTimeout, conn, "udp", func() (net.Conn, error) {
				return ns.DialUDP(m.Mapped)
			})
		}(mapping)
	}
}

// StartRemoteUDP открывает локальный UDP-сервис в сети Yggdrasil.
func StartRemoteUDP(node NodeInterface, mappings []types.UDPMapping, sessionTimeout time.Duration) {
	log := node.GetLogger()
	ns := node.GetNetstack()
	wg := node.GetComponentsWg()

	for _, mapping := range mappings {
		wg.Add(1)
		go func(m types.UDPMapping) {
			defer wg.Done()
			conn, err := ns.ListenUDP(m.Listen)
			if err != nil {
				log.Errorf("Failed to listen on Yggdrasil UDP %s: %s", m.Listen, err)
				return
			}
			node.AddCloser(conn)
			log.Infof("Mapping Yggdrasil UDP port %d to %s", m.Listen.Port, m.Mapped)
			runUDPLoop(node, sessionTimeout, conn, "udp-remote", func() (net.Conn, error) {
				return net.DialUDP("udp", nil, m.Mapped)
			})
		}(mapping)
	}
}

// runUDPLoop читает входящие пакеты и управляет UDP-сессиями по источнику.
// dialFn открывает новое upstream-соединение для нового адреса источника.
func runUDPLoop(
	node NodeInterface,
	sessionTimeout time.Duration,
	listenConn net.PacketConn,
	connIdPrefix string,
	dialFn func() (net.Conn, error),
) {
	log := node.GetLogger()
	cb := node.GetActivityCallback()
	counter := node.GetConnCounter()
	wg := node.GetComponentsWg()
	ctx := node.GetComponentsCtx()
	mtu := node.GetCoreMTU()

	connections := NewUDPSessionMap()
	wg.Add(1)
	go func() {
		defer wg.Done()
		CleanupUDPSessions(ctx, connections, sessionTimeout, log)
	}()

	buf := make([]byte, mtu)
	for {
		n, remoteAddr, err := listenConn.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Debugf("UDP read error: %v", err)
			continue
		}
		if n == 0 {
			continue
		}

		key := remoteAddr.String()
		session, ok := connections.Load(key)
		if !ok {
			log.Debugf("Creating new session for %s", key)
			fwdConn, err := dialFn()
			if err != nil {
				log.Errorf("Failed to connect to upstream: %s", err)
				continue
			}
			sessCtx, sessCancel := context.WithCancel(ctx)
			session = &UDPSessionObj{
				Conn:       fwdConn,
				RemoteAddr: remoteAddr,
				cancel:     sessCancel,
			}
			if cb != nil {
				session.ConnId = activity.GenerateConnId(connIdPrefix, key)
				session.Callback = cb
				session.Counter = counter
				cb.OnConnectionCreated(session.ConnId, "udp")
				counter.Increment()
			}
			session.LastActivity.Store(time.Now().UnixMilli())
			connections.Store(key, session)
			go types.ReverseProxyUDP(sessCtx, mtu, listenConn, remoteAddr, fwdConn)
		}

		session.LastActivity.Store(time.Now().UnixMilli())
		if _, err = session.Conn.Write(buf[:n]); err != nil {
			log.Debugf("Session write error: %s", err)
			CloseUDPSession(session, log)
			connections.Delete(key)
		}
	}
}

// //

// CloseUDPSession отменяет контекст сессии и закрывает соединение.
// Уведомляет callback активности, если он зарегистрирован.
func CloseUDPSession(session *UDPSessionObj, logger core.Logger) {
	session.CloseOnce.Do(func() {
		// Сначала отменяет контекст — разблокирует ожидающий Read в ReverseProxyUDP через deadline.
		if session.cancel != nil {
			session.cancel()
		}
		_ = session.Conn.Close()
		if session.Callback != nil && session.ConnId != "" {
			session.Callback.OnConnectionClosed(session.ConnId)
			session.Counter.Decrement()
		}
	})
}

// CleanupUDPSessions периодически удаляет неактивные UDP-сессии.
// При отмене контекста закрывает все оставшиеся сессии для разблокировки горутин ReverseProxyUDP.
func CleanupUDPSessions(ctx context.Context, connections *UDPSessionMapObj, timeout time.Duration, logger core.Logger) {
	ticker := time.NewTicker(timeout / 4)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			connections.Range(func(key string, session *UDPSessionObj) bool {
				CloseUDPSession(session, logger)
				return true
			})
			return
		case <-ticker.C:
			now := time.Now().UnixMilli()
			connections.Range(func(key string, session *UDPSessionObj) bool {
				if now-session.LastActivity.Load() > timeout.Milliseconds() {
					logger.Debugf("Cleaning up inactive UDP session %s", key)
					CloseUDPSession(session, logger)
					connections.Delete(key)
				}
				return true
			})
		}
	}
}
