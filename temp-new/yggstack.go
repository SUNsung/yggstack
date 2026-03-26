package yggstack

import (
	"sync"

	yggcore "github.com/yggdrasil-network/yggdrasil-go/src/core"

	"github.com/yggdrasil-network/yggstack/temp-new/mod/core"
	"github.com/yggdrasil-network/yggstack/temp-new/mod/resolver"
	"github.com/yggdrasil-network/yggstack/temp-new/mod/socks"
)

// // // // // // // // // //

// Obj — узел Yggdrasil для встраивания в приложения.
// Объединяет ядро (DialContext/Listen), резолвер (.pk.ygg) и SOCKS5.
// Все сетевые методы ядра доступны напрямую через встраивание интерфейса.
// Multicast и Admin доступны через core.Interface
type Obj struct {
	core.Interface
	socksServer socks.Interface
	logger      yggcore.Logger
	done        chan struct{}
	closeOnce   sync.Once
}

// New создаёт и запускает узел
func New(cfg ConfigObj) (*Obj, error) {
	coreNode, err := core.New(core.ConfigObj{
		Config:          cfg.Config,
		Logger:          cfg.Logger,
		CoreStopTimeout: cfg.CoreStopTimeout,
	})
	if err != nil {
		return nil, err
	}

	obj := &Obj{
		Interface:   coreNode,
		socksServer: socks.New(coreNode),
		logger:      cfg.Logger,
		done:        make(chan struct{}),
	}

	// Автозавершение при отмене контекста
	if cfg.Ctx != nil {
		go func() {
			select {
			case <-cfg.Ctx.Done():
				obj.Close()
			case <-obj.done:
			}
		}()
	}

	return obj, nil
}

// //

// EnableSOCKS запускает SOCKS5-прокси с указанными параметрами.
// Резолвер создаётся автоматически на основе cfg.Nameserver
func (o *Obj) EnableSOCKS(cfg SOCKSConfigObj) error {
	res := resolver.New(o.Interface, cfg.Nameserver)

	return o.socksServer.Enable(socks.EnableConfigObj{
		Addr:           cfg.Addr,
		Resolver:       res,
		Verbose:        cfg.Verbose,
		Logger:         o.logger,
		MaxConnections: cfg.MaxConnections,
	})
}

// DisableSOCKS останавливает SOCKS5-прокси
func (o *Obj) DisableSOCKS() error {
	return o.socksServer.Disable()
}

// Close корректно останавливает SOCKS и ядро; безопасен для повторного вызова
func (o *Obj) Close() error {
	o.closeOnce.Do(func() {
		close(o.done)
		if err := o.socksServer.Disable(); err != nil && o.logger != nil {
			o.logger.Warnf("socks disable: %v", err)
		}
		if err := o.Interface.Close(); err != nil && o.logger != nil {
			o.logger.Warnf("core close: %v", err)
		}
	})
	return nil
}
