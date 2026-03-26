package socks

import (
	"context"
	"net"
)

// // // // // // // // // //

// NetworkInterface — сетевые возможности, необходимые SOCKS-серверу
type NetworkInterface interface {
	DialContext(ctx context.Context, network, addr string) (net.Conn, error)
}

// ResolverInterface — резолвинг имён для SOCKS-сервера.
// Сигнатура совпадает с socks5.NameResolver для прямой подстановки
type ResolverInterface interface {
	Resolve(ctx context.Context, name string) (context.Context, net.IP, error)
}

// LoggerInterface — логирование SOCKS-сервера.
// Совместим с socks5.Logger (Errorf) + дополнительный Infof
type LoggerInterface interface {
	Infof(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// // // //

// ObjInterface — контракт SOCKS5-сервера
type ObjInterface interface {
	Enable(cfg EnableConfigObj) error
	Disable() error
}
