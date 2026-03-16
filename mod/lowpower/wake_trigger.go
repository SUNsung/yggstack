package lowpower

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/yggdrasil-network/yggstack/src/types"
)

// // // // // // // // // //

// wakeTriggerObj слушает порт SOCKS пока узел спит.
// При входящем соединении будит узел и проксирует соединение через SOCKS.
type wakeTriggerObj struct {
	listener net.Listener
	mu       sync.Mutex
	wg       sync.WaitGroup
}

// //

const (
	wakeSOCKSWaitTimeout = 10 * time.Second
	wakeSOCKSDialTimeout = 5 * time.Second
)

// //

func (m *ManagerObj) startWakeTrigger(addr string) error {
	network := "tcp"
	if m.node.SocksIsUnix() {
		network = "unix"
		// Удаляет устаревший сокет, оставшийся от SOCKS.
		_ = os.Remove(addr)
	}
	listener, err := net.Listen(network, addr)
	if err != nil {
		return fmt.Errorf("wake trigger listen %s %s: %w", network, addr, err)
	}
	m.wakeTrigger.mu.Lock()
	m.wakeTrigger.listener = listener
	m.wakeTrigger.mu.Unlock()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			// Проверяет под блокировкой, что слушатель ещё жив — предотвращает гонку wg.Add и wg.Wait.
			m.wakeTrigger.mu.Lock()
			if m.wakeTrigger.listener == nil {
				m.wakeTrigger.mu.Unlock()
				conn.Close()
				return
			}
			m.wakeTrigger.wg.Add(1)
			m.wakeTrigger.mu.Unlock()
			go m.handleWakeConnection(conn)
		}
	}()
	return nil
}

// handleWakeConnection будит узел, ожидает готовности SOCKS и проксирует соединение.
// ProxyTCP закрывает оба соединения, поэтому ранние возвраты закрывают conn явно.
func (m *ManagerObj) handleWakeConnection(conn net.Conn) {
	defer m.wakeTrigger.wg.Done()

	m.TransitionToFullPower()

	network := "tcp"
	if m.node.SocksIsUnix() {
		network = "unix"
	}

	select {
	case <-m.node.SocksReadyCh():
	case <-time.After(m.socksWaitTimeout):
		m.logger.Errorf("Low power mode: SOCKS not ready after %s, dropping wake connection", m.socksWaitTimeout)
		_ = conn.Close()
		return
	case <-m.ctx.Done():
		_ = conn.Close()
		return
	}

	socksConn, err := net.DialTimeout(network, m.node.SocksAddr(), wakeSOCKSDialTimeout)
	if err != nil {
		m.logger.Errorf("Low power mode: failed to connect to SOCKS %s: %s", m.node.SocksAddr(), err)
		_ = conn.Close()
		return
	}

	// Закрывает соединения при отмене контекста, чтобы разблокировать ProxyTCP.
	// Без этого Stop() зависнет на wg.Wait() пока io.Copy блокирован.
	proxyDone := make(chan struct{})
	go func() {
		select {
		case <-m.ctx.Done():
			_ = conn.Close()
			_ = socksConn.Close()
		case <-proxyDone:
		}
	}()

	types.ProxyTCP(conn, socksConn)
	close(proxyDone)
}

// closeWakeListener закрывает слушатель без ожидания горутин.
// Используется при пробуждении — новые соединения не принимаются,
// но горутины handleWakeConnection продолжают работу.
func (m *ManagerObj) closeWakeListener() {
	m.wakeTrigger.mu.Lock()
	if m.wakeTrigger.listener != nil {
		_ = m.wakeTrigger.listener.Close()
		if m.node.SocksIsUnix() {
			_ = os.Remove(m.node.SocksAddr())
		}
		m.wakeTrigger.listener = nil
	}
	m.wakeTrigger.mu.Unlock()
}

// stopWakeTrigger закрывает слушатель и ожидает завершения всех горутин handleWakeConnection.
func (m *ManagerObj) stopWakeTrigger() {
	m.closeWakeListener()
	m.wakeTrigger.wg.Wait()
}
