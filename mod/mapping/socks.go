package mapping

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/things-go/go-socks5"

	"github.com/yggdrasil-network/yggstack/mod/activity"
	"github.com/yggdrasil-network/yggstack/src/types"
)

// // // // // // // // // //

// removeUnixSocket удаляет файл Unix-сокета.
// Возвращает ошибку если путь — символическая ссылка, во избежание
// перенаправления удаления на произвольный путь файловой системы злоумышленником.
func removeUnixSocket(path string) error {
	fi, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("os.Lstat %s: %w", path, err)
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("refusing to remove %s: is a symlink", path)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("os.Remove %s: %w", path, err)
	}
	return nil
}

// //

// SocksConfigObj хранит параметры сервера SOCKS5.
type SocksConfigObj struct {
	Addr       string
	Nameserver string
	Verbose    bool
}

// SocksResultObj хранит результат запуска сервера SOCKS5.
type SocksResultObj struct {
	Listener net.Listener
	IsUnix   bool
}

// //

// StartSocks запускает сервер SOCKS5 и возвращает слушатель.
// readyCh закрывается когда слушатель готов принимать соединения.
func StartSocks(node NodeInterface, cfg SocksConfigObj, readyCh chan struct{}) (*SocksResultObj, error) {
	ns := node.GetNetstack()
	log := node.GetLogger()
	cb := node.GetActivityCallback()
	counter := node.GetConnCounter()

	dialFn := ns.DialContext
	if cb != nil {
		originalDial := dialFn
		dialFn = func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := originalDial(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			connId := activity.GenerateConnId("socks", addr)
			cb.OnConnectionCreated(connId, "socks")
			counter.Increment()
			return &activity.TrackedConnObj{
				Conn: conn, ConnId: connId,
				Callback: cb, Counter: counter,
			}, nil
		}
	}

	socksOptions := []socks5.Option{
		socks5.WithDial(dialFn),
	}
	if cfg.Nameserver == "" {
		log.Infof("DNS nameserver is not set!")
		log.Infof("SOCKS server will not be able to resolve hostnames other than .pk.ygg !")
	}
	resolver := types.NewNameResolver(ns, cfg.Nameserver)
	socksOptions = append(socksOptions, socks5.WithResolver(resolver))
	if cfg.Verbose {
		socksOptions = append(socksOptions, socks5.WithLogger(log))
	}
	server := socks5.NewServer(socksOptions...)

	result := &SocksResultObj{}

	if strings.Contains(cfg.Addr, ":") {
		log.Infof("Starting SOCKS server on %s", cfg.Addr)
		var err error
		result.Listener, err = net.Listen("tcp", cfg.Addr)
		if err != nil {
			return nil, fmt.Errorf("net.Listen tcp %s: %w", cfg.Addr, err)
		}
	} else {
		result.IsUnix = true
		log.Infof("Starting SOCKS server with socket file %s", cfg.Addr)
		var err error
		result.Listener, err = net.Listen("unix", cfg.Addr)
		if err != nil {
			if isErrorAddressAlreadyInUse(err) {
				probeConn, dialErr := net.Dial("unix", cfg.Addr)
				if dialErr != nil {
					if rmErr := removeUnixSocket(cfg.Addr); rmErr != nil {
						return nil, rmErr
					}
					result.Listener, err = net.Listen("unix", cfg.Addr)
					if err != nil {
						return nil, fmt.Errorf("net.Listen unix %s: %w", cfg.Addr, err)
					}
				} else {
					_ = probeConn.Close()
					return nil, fmt.Errorf("another instance is listening on socket '%s'", cfg.Addr)
				}
			} else {
				return nil, fmt.Errorf("net.Listen unix %s: %w", cfg.Addr, err)
			}
		}
	}

	// Сигнализирует handleWakeConnection: SOCKS готов.
	if readyCh != nil {
		close(readyCh)
	}

	wg := node.GetComponentsWg()
	ctx := node.GetComponentsCtx()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := server.Serve(result.Listener); err != nil {
			if ctx.Err() == nil {
				log.Errorf("SOCKS5 server error: %s", err)
			}
		}
	}()

	return result, nil
}
