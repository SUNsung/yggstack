package mobile

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	yggcore "github.com/yggdrasil-network/yggdrasil-go/src/core"

	"github.com/yggdrasil-network/yggstack/temp-new/mod/core"
)

// // // // // // // // // //

const proxyTCPCloseTimeout = 5 * time.Second

// //

func proxyTCP(c1, c2 net.Conn) {
	errCh := make(chan error, 2)
	go func() { _, err := io.Copy(c1, c2); errCh <- err }()
	go func() { _, err := io.Copy(c2, c1); errCh <- err }()

	<-errCh
	_ = c1.Close()
	_ = c2.Close()

	select {
	case <-errCh:
	case <-time.After(proxyTCPCloseTimeout):
	}
}

// //

func startLocalTCP(ctx context.Context, node core.Interface, mappings []tcpMappingObj, log yggcore.Logger, wg *sync.WaitGroup) {
	for _, m := range mappings {
		wg.Add(1)
		go func(m tcpMappingObj) {
			defer wg.Done()
			listener, err := net.ListenTCP("tcp", m.Listen)
			if err != nil {
				log.Errorf("Failed to listen on local TCP %s: %s", m.Listen, err)
				return
			}
			defer listener.Close()
			log.Infof("Mapping local TCP port %d to Yggdrasil %s", m.Listen.Port, m.Mapped)

			go func() {
				<-ctx.Done()
				listener.Close()
			}()

			for {
				c, err := listener.Accept()
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Errorf("Local TCP accept error: %s", err)
					return
				}
				remote, err := node.DialContext(ctx, "tcp", fmt.Sprintf("[%s]:%d", m.Mapped.IP, m.Mapped.Port))
				if err != nil {
					log.Errorf("Failed to connect to %s: %s", m.Mapped, err)
					_ = c.Close()
					continue
				}
				go proxyTCP(c, remote)
			}
		}(m)
	}
}

func startRemoteTCP(ctx context.Context, node core.Interface, mappings []tcpMappingObj, log yggcore.Logger, wg *sync.WaitGroup) {
	for _, m := range mappings {
		wg.Add(1)
		go func(m tcpMappingObj) {
			defer wg.Done()
			addr := fmt.Sprintf("[%s]:%d", node.Address(), m.Listen.Port)
			listener, err := node.Listen("tcp", addr)
			if err != nil {
				log.Errorf("Failed to listen on Yggdrasil TCP %s: %s", addr, err)
				return
			}
			defer listener.Close()
			log.Infof("Mapping Yggdrasil TCP port %d to %s", m.Listen.Port, m.Mapped)

			go func() {
				<-ctx.Done()
				listener.Close()
			}()

			for {
				c, err := listener.Accept()
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Errorf("Remote TCP accept error: %s", err)
					return
				}
				remote, err := net.DialTCP("tcp", nil, m.Mapped)
				if err != nil {
					log.Errorf("Failed to connect to %s: %s", m.Mapped, err)
					_ = c.Close()
					continue
				}
				go proxyTCP(c, remote)
			}
		}(m)
	}
}

// //

func startLocalUDP(ctx context.Context, node core.Interface, mappings []udpMappingObj, sessionTimeout time.Duration, log yggcore.Logger, wg *sync.WaitGroup) {
	for _, m := range mappings {
		wg.Add(1)
		go func(m udpMappingObj) {
			defer wg.Done()
			conn, err := net.ListenUDP("udp", m.Listen)
			if err != nil {
				log.Errorf("Failed to listen on local UDP %s: %s", m.Listen, err)
				return
			}
			defer conn.Close()
			log.Infof("Mapping local UDP port %d to Yggdrasil %s", m.Listen.Port, m.Mapped)

			go func() {
				<-ctx.Done()
				conn.Close()
			}()

			runUDPLoop(ctx, conn, func() (net.Conn, error) {
				return node.DialContext(ctx, "udp", fmt.Sprintf("[%s]:%d", m.Mapped.IP, m.Mapped.Port))
			}, node.MTU(), sessionTimeout, log)
		}(m)
	}
}

func startRemoteUDP(ctx context.Context, node core.Interface, mappings []udpMappingObj, sessionTimeout time.Duration, log yggcore.Logger, wg *sync.WaitGroup) {
	for _, m := range mappings {
		wg.Add(1)
		go func(m udpMappingObj) {
			defer wg.Done()
			addr := fmt.Sprintf("[%s]:%d", node.Address(), m.Listen.Port)
			conn, err := node.ListenPacket("udp", addr)
			if err != nil {
				log.Errorf("Failed to listen on Yggdrasil UDP %s: %s", addr, err)
				return
			}
			defer conn.Close()
			log.Infof("Mapping Yggdrasil UDP port %d to %s", m.Listen.Port, m.Mapped)

			go func() {
				<-ctx.Done()
				conn.Close()
			}()

			runUDPLoop(ctx, conn, func() (net.Conn, error) {
				return net.DialUDP("udp", nil, m.Mapped)
			}, node.MTU(), sessionTimeout, log)
		}(m)
	}
}

// //

type udpSessionObj struct {
	conn         net.Conn
	lastActivity int64
	cancel       context.CancelFunc
	closeOnce    sync.Once
}

func runUDPLoop(ctx context.Context, listenConn net.PacketConn, dialFn func() (net.Conn, error), mtu uint64, sessionTimeout time.Duration, log yggcore.Logger) {
	sessions := sync.Map{}

	// Очистка неактивных сессий
	go func() {
		ticker := time.NewTicker(sessionTimeout / 4)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				sessions.Range(func(_, v any) bool {
					s := v.(*udpSessionObj)
					s.closeOnce.Do(func() {
						s.cancel()
						_ = s.conn.Close()
					})
					return true
				})
				return
			case <-ticker.C:
				now := time.Now().UnixMilli()
				sessions.Range(func(k, v any) bool {
					s := v.(*udpSessionObj)
					if now-s.lastActivity > sessionTimeout.Milliseconds() {
						log.Debugf("Cleaning up inactive UDP session %s", k)
						s.closeOnce.Do(func() {
							s.cancel()
							_ = s.conn.Close()
						})
						sessions.Delete(k)
					}
					return true
				})
			}
		}
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
		val, ok := sessions.Load(key)
		if !ok {
			fwdConn, err := dialFn()
			if err != nil {
				log.Errorf("Failed to connect to upstream: %s", err)
				continue
			}
			sessCtx, sessCancel := context.WithCancel(ctx)
			session := &udpSessionObj{
				conn:         fwdConn,
				lastActivity: time.Now().UnixMilli(),
				cancel:       sessCancel,
			}
			sessions.Store(key, session)
			go reverseProxyUDP(sessCtx, mtu, listenConn, remoteAddr, fwdConn)
			val = session
		}

		session := val.(*udpSessionObj)
		session.lastActivity = time.Now().UnixMilli()
		if _, err = session.conn.Write(buf[:n]); err != nil {
			log.Debugf("Session write error: %s", err)
			session.closeOnce.Do(func() {
				session.cancel()
				_ = session.conn.Close()
			})
			sessions.Delete(key)
		}
	}
}

func reverseProxyUDP(ctx context.Context, mtu uint64, dst net.PacketConn, dstAddr net.Addr, src net.Conn) {
	watchDone := make(chan struct{})
	defer close(watchDone)
	go func() {
		select {
		case <-ctx.Done():
			_ = src.SetReadDeadline(time.Now())
		case <-watchDone:
		}
	}()

	buf := make([]byte, mtu)
	for {
		n, err := src.Read(buf)
		if err != nil {
			return
		}
		if n > 0 {
			_, _ = dst.WriteTo(buf[:n], dstAddr)
		}
	}
}
