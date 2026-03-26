package core

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/url"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	golog "github.com/gologme/log"
	"github.com/yggdrasil-network/yggdrasil-go/src/admin"
	"github.com/yggdrasil-network/yggdrasil-go/src/config"
	yggcore "github.com/yggdrasil-network/yggdrasil-go/src/core"
	"github.com/yggdrasil-network/yggdrasil-go/src/multicast"
)

// // // // // // // // // //

var ErrNotAvailable = fmt.Errorf("netstack is not available")

// Obj — узел Yggdrasil с userspace TCP/UDP стеком.
// Предоставляет стандартные Go-сетевые методы: DialContext, Listen, ListenPacket
type Obj struct {
	core        *yggcore.Core
	nodeCfg     *config.NodeConfig
	netstackPtr atomic.Pointer[netstackObj]
	logger      yggcore.Logger
	multicast   *multicast.Multicast
	adminSocket *admin.AdminSocket
	mu          sync.Mutex
	closeOnce   sync.Once
	closers     []io.Closer
	closersMu   sync.Mutex
	coreTimeout time.Duration
}

// New создаёт и запускает узел Yggdrasil.
// Для корректного завершения вызывающий обязан вызвать Close()
func New(cfg ConfigObj) (*Obj, error) {
	log := cfg.Logger
	if log == nil {
		log = noopLoggerObj{}
	}

	nodeCfg := cfg.Config
	if nodeCfg == nil {
		nodeCfg = config.GenerateConfig()
		nodeCfg.AdminListen = "none"
	}

	obj := &Obj{
		nodeCfg:     nodeCfg,
		logger:      log,
		coreTimeout: cfg.CoreStopTimeout,
	}

	// Ядро Yggdrasil
	var err error
	obj.core, err = yggcore.New(nodeCfg.Certificate, log, buildCoreOptions(nodeCfg)...)
	if err != nil {
		return nil, fmt.Errorf("core.New: %w", err)
	}

	// Сетевой стек
	ns, err := newNetstack(obj.core, log)
	if err != nil {
		obj.core.Stop()
		return nil, fmt.Errorf("netstack: %w", err)
	}
	obj.netstackPtr.Store(ns)

	log.Infof("Address: %s", obj.Address())
	log.Infof("Subnet: %s", obj.Subnet())
	log.Infof("Public key: %s", hex.EncodeToString(obj.core.PublicKey()))

	return obj, nil
}

// //

// Close корректно останавливает узел; безопасен для повторного вызова
func (o *Obj) Close() error {
	o.closeOnce.Do(func() {
		o.mu.Lock()

		// Multicast и admin — до закрытия core
		if o.multicast != nil {
			_ = o.multicast.Stop()
			o.multicast = nil
		}
		if o.adminSocket != nil {
			_ = o.adminSocket.Stop()
			o.adminSocket = nil
		}

		o.mu.Unlock()

		// Зарегистрированные ресурсы (listeners и т.д.)
		o.closersMu.Lock()
		for _, c := range o.closers {
			_ = c.Close()
		}
		o.closers = nil
		o.closersMu.Unlock()

		// Core останавливается до netstack: ipv6rwc.Read() разблокируется
		// только после core.Stop()
		o.stopCore()

		if ns := o.netstackPtr.Swap(nil); ns != nil {
			ns.close()
		}
	})
	return nil
}

// //

// DialContext открывает соединение к Yggdrasil-адресу.
// Совместим с http.Transport.DialContext для использования как HTTP-транспорт
func (o *Obj) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	ns := o.netstackPtr.Load()
	if ns == nil {
		return nil, ErrNotAvailable
	}
	return ns.DialContext(ctx, network, address)
}

// Listen создаёт TCP listener на Yggdrasil-адресе.
// Формат адреса: ":port" или "[ipv6]:port".
// Listener автоматически закрывается при Close()
func (o *Obj) Listen(network, address string) (net.Listener, error) {
	ns := o.netstackPtr.Load()
	if ns == nil {
		return nil, ErrNotAvailable
	}
	ln, err := ns.Listen(network, address)
	if err != nil {
		return nil, err
	}
	o.addCloser(ln)
	return ln, nil
}

// ListenPacket создаёт UDP listener на Yggdrasil-адресе.
// Формат адреса: ":port" или "[ipv6]:port".
// Listener автоматически закрывается при Close()
func (o *Obj) ListenPacket(network, address string) (net.PacketConn, error) {
	ns := o.netstackPtr.Load()
	if ns == nil {
		return nil, ErrNotAvailable
	}
	pc, err := ns.ListenPacket(network, address)
	if err != nil {
		return nil, err
	}
	o.addCloser(pc)
	return pc, nil
}

// //

// Address — IPv6-адрес узла в диапазоне 200::/7
func (o *Obj) Address() net.IP {
	if o.core == nil {
		return nil
	}
	addr := o.core.Address()
	return net.IP(addr[:])
}

// Subnet — маршрутизируемая /64 подсеть узла в диапазоне 300::/7
func (o *Obj) Subnet() net.IPNet {
	if o.core == nil {
		return net.IPNet{}
	}
	return o.core.Subnet()
}

// PublicKey — ed25519 публичный ключ узла (32 байта)
func (o *Obj) PublicKey() ed25519.PublicKey {
	if o.core == nil {
		return nil
	}
	return o.core.PublicKey()
}

// MTU — MTU сетевого интерфейса
func (o *Obj) MTU() uint64 {
	ns := o.netstackPtr.Load()
	if ns == nil {
		return 0
	}
	return ns.MTU()
}

// //

// AddPeer добавляет пир в runtime. URI: "tcp://host:port", "quic://host:port"
func (o *Obj) AddPeer(uri string) error {
	if o.core == nil {
		return ErrNotAvailable
	}
	u, err := url.Parse(uri)
	if err != nil {
		return fmt.Errorf("url.Parse: %w", err)
	}
	return o.core.AddPeer(u, "")
}

// RemovePeer удаляет пир в runtime
func (o *Obj) RemovePeer(uri string) error {
	if o.core == nil {
		return ErrNotAvailable
	}
	u, err := url.Parse(uri)
	if err != nil {
		return fmt.Errorf("url.Parse: %w", err)
	}
	return o.core.RemovePeer(u, "")
}

// //

// EnableMulticast включает mDNS-обнаружение пиров в локальной сети.
// Интерфейсы берутся из NodeConfig.MulticastInterfaces.
// logger — специфичный для multicast (upstream требует *golog.Logger)
func (o *Obj) EnableMulticast(logger *golog.Logger) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.multicast != nil {
		return fmt.Errorf("multicast already enabled")
	}

	var options []multicast.SetupOption
	for _, intf := range o.nodeCfg.MulticastInterfaces {
		re, err := regexp.Compile(intf.Regex)
		if err != nil {
			return fmt.Errorf("invalid multicast regex %q: %w", intf.Regex, err)
		}
		options = append(options, multicast.MulticastInterface{
			Regex:    re,
			Beacon:   intf.Beacon,
			Listen:   intf.Listen,
			Port:     intf.Port,
			Priority: uint8(intf.Priority),
			Password: intf.Password,
		})
	}

	var err error
	o.multicast, err = multicast.New(o.core, logger, options...)
	if err != nil {
		return fmt.Errorf("multicast.New: %w", err)
	}

	// Регистрация admin-хендлеров если admin уже включён
	if o.adminSocket != nil {
		o.multicast.SetupAdminHandlers(o.adminSocket)
	}

	return nil
}

// DisableMulticast останавливает mDNS-обнаружение
func (o *Obj) DisableMulticast() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.multicast == nil {
		return nil
	}
	err := o.multicast.Stop()
	o.multicast = nil
	return err
}

// //

// EnableAdmin запускает admin-сокет на указанном адресе.
// Формат: "unix:///path" или "tcp://host:port"
func (o *Obj) EnableAdmin(addr string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.adminSocket != nil {
		return fmt.Errorf("admin already enabled")
	}

	var err error
	o.adminSocket, err = admin.New(o.core, o.logger, admin.ListenAddress(addr))
	if err != nil {
		return fmt.Errorf("admin.New: %w", err)
	}
	if o.adminSocket != nil {
		o.adminSocket.SetupAdminHandlers()
	}

	// Регистрация multicast-хендлеров если multicast уже включён
	if o.multicast != nil && o.adminSocket != nil {
		o.multicast.SetupAdminHandlers(o.adminSocket)
	}

	return nil
}

// DisableAdmin останавливает admin-сокет
func (o *Obj) DisableAdmin() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.adminSocket == nil {
		return nil
	}
	err := o.adminSocket.Stop()
	o.adminSocket = nil
	return err
}

// //

func (o *Obj) addCloser(c io.Closer) {
	o.closersMu.Lock()
	o.closers = append(o.closers, c)
	o.closersMu.Unlock()
}

func (o *Obj) stopCore() {
	if o.core == nil {
		return
	}
	if o.coreTimeout == 0 {
		o.core.Stop()
		o.core = nil
		return
	}
	done := make(chan struct{})
	go func() {
		o.core.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(o.coreTimeout):
		o.logger.Warnf("core.Stop() timed out after %s", o.coreTimeout)
	}
	o.core = nil
}

func buildCoreOptions(cfg *config.NodeConfig) []yggcore.SetupOption {
	var opts []yggcore.SetupOption
	opts = append(opts, yggcore.NodeInfo(cfg.NodeInfo))
	opts = append(opts, yggcore.NodeInfoPrivacy(cfg.NodeInfoPrivacy))
	for _, addr := range cfg.Listen {
		opts = append(opts, yggcore.ListenAddress(addr))
	}
	for _, peer := range cfg.Peers {
		opts = append(opts, yggcore.Peer{URI: peer})
	}
	for intf, peers := range cfg.InterfacePeers {
		for _, peer := range peers {
			opts = append(opts, yggcore.Peer{URI: peer, SourceInterface: intf})
		}
	}
	for _, allowed := range cfg.AllowedPublicKeys {
		k, err := hex.DecodeString(allowed)
		if err != nil {
			continue
		}
		opts = append(opts, yggcore.AllowedPublicKey(k[:]))
	}
	return opts
}

// //

// noopLoggerObj — логгер-заглушка при отсутствии пользовательского логгера
type noopLoggerObj struct{}

func (noopLoggerObj) Printf(string, ...interface{}) {}
func (noopLoggerObj) Println(...interface{})        {}
func (noopLoggerObj) Infof(string, ...interface{})  {}
func (noopLoggerObj) Infoln(...interface{})         {}
func (noopLoggerObj) Warnf(string, ...interface{})  {}
func (noopLoggerObj) Warnln(...interface{})         {}
func (noopLoggerObj) Errorf(string, ...interface{}) {}
func (noopLoggerObj) Errorln(...interface{})        {}
func (noopLoggerObj) Debugf(string, ...interface{}) {}
func (noopLoggerObj) Debugln(...interface{})        {}
func (noopLoggerObj) Tracef(string, ...interface{}) {}
func (noopLoggerObj) Traceln(...interface{})        {}
