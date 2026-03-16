package yggstack

import (
	"context"
	"time"

	golog "github.com/gologme/log"

	"github.com/yggdrasil-network/yggdrasil-go/src/config"
	"github.com/yggdrasil-network/yggdrasil-go/src/core"

	"github.com/yggdrasil-network/yggstack/mod/activity"
	"github.com/yggdrasil-network/yggstack/mod/lowpower"
	"github.com/yggdrasil-network/yggstack/mod/mapping"
	"github.com/yggdrasil-network/yggstack/mod/peers"
	"github.com/yggdrasil-network/yggstack/src/types"
)

// // // // // // // // // //

type ConfigObj struct {
	// Ctx — родительский контекст жизненного цикла узла.
	// При отмене узел завершает работу корректно.
	// Если nil, используется context.Background().
	Ctx context.Context

	// Config — конфигурация узла Yggdrasil (ключи, пиры, адреса прослушивания и т.д.).
	// Генерируется через config.GenerateConfig(). Пиры задаются в URI-формате:
	// tcp://host:port, tls://host:port, quic://host:port, ws://host:port, wss://host:port.
	// Если nil, генерируется новая конфигурация со случайными ключами и AdminListen="none".
	Config *config.NodeConfig

	// Logger получает весь лог-вывод узла (подключения пиров, ошибки и т.д.).
	// Должен реализовывать интерфейс core.Logger из yggdrasil-go (Println, Infof, Warnf, Errorf и т.д.).
	// Если nil, весь лог-вывод отбрасывается.
	Logger core.Logger

	// MulticastLogger включает mDNS-обнаружение пиров в локальной сети.
	// Требует *log.Logger (не core.Logger) из-за сигнатуры multicast.New() в upstream.
	// Если nil, multicast полностью отключён.
	// TODO: switch to core.Logger when multicast.New() accepts an interface instead of *log.Logger
	MulticastLogger *golog.Logger

	// SocksAddr запускает SOCKS5-прокси-сервер на указанном адресе.
	// TCP-формат: "127.0.0.1:1080" или ":1080".
	// UNIX-сокет: "/tmp/yggstack.sock" (без двоеточия в строке).
	// Поддерживает команды CONNECT (TCP) и UDP ASSOCIATE.
	// Автоматически разрешает домены <publickey>.pk.ygg в IPv6-адреса Yggdrasil.
	// Если пусто, SOCKS5-прокси не запускается.
	SocksAddr string

	// Nameserver — DNS-сервер, доступный через Yggdrasil, для разрешения доменов .ygg через SOCKS5.
	// Формат: "[ipv6]:port", например "[324:71e:281a:9ed3::53]:53".
	// Без него разрешаются только имена <publickey>.pk.ygg; остальные .ygg-домены будут недоступны.
	// Используется только когда задан SocksAddr.
	Nameserver string

	// SocksVerbose включает подробное логирование соединений SOCKS5.
	// Используется только когда задан SocksAddr.
	SocksVerbose bool

	// Mapping содержит все правила перенаправления портов (локальные и удалённые, TCP и UDP).
	Mapping MappingConfigObj

	// UDPSessionTimeout — таймаут неактивности для UDP-сессий перенаправления.
	// По истечении этого времени без трафика сессия закрывается и ресурсы освобождаются.
	// По умолчанию: 120 с.
	UDPSessionTimeout time.Duration

	// ActivityCallback получает уведомления о жизненном цикле соединений (создание/передача/закрытие).
	// Если nil, соединения не оборачиваются — без накладных расходов.
	ActivityCallback activity.CallbackInterface

	// PeerChangeCallback получает уведомления при изменении числа подключённых пиров.
	// Использует адаптивный опрос: 500 мс при активных соединениях, 5 с в простое.
	// Если nil, мониторинг пиров отключён.
	PeerChangeCallback peers.ChangeCallbackInterface

	// CoreStopTimeout ограничивает время ожидания завершения core.Stop().
	// При превышении таймаута завершение продолжается без ожидания.
	// Актуально при смене сети (WiFi → LTE), когда закрытие пиров может зависнуть навсегда.
	// Если 0 — ждёт вечно (по умолчанию, обратная совместимость).
	CoreStopTimeout time.Duration

	// LowPower включает энергосбережение: когда нет активных соединений дольше чем
	// IdleTimeout, узел останавливается. При входящем соединении — перезапускается автоматически.
	// nil = отключено. Требует ActivityCallback != nil.
	LowPower *lowpower.ConfigObj

	// NodeMapping overrides the default mapping.NodeInterface implementation.
	// Controls how SOCKS5, TCP/UDP forwarding interact with the node (netstack, logging, etc.).
	// When nil, the built-in adapter that delegates to *Obj is used.
	NodeMapping mapping.NodeInterface

	// NodeControl overrides the default lowpower.NodeControlInterface implementation.
	// Controls how low power mode stops/starts node components.
	// When nil, the built-in adapter that delegates to *Obj is used.
	// Only relevant when LowPower is enabled.
	NodeControl lowpower.NodeControlInterface
}

// //

// MappingConfigObj holds all port forwarding rules.
type MappingConfigObj struct {
	// LocalTCP forwards a local TCP port to a remote Yggdrasil address (like ssh -L).
	// Each entry maps Listen (local host:port) -> Mapped (remote Yggdrasil IPv6:port).
	// Example: listen 127.0.0.1:8080 -> forward to [ygg-ipv6]:8080.
	// CLI equivalent: -local-tcp 127.0.0.1:8080:<remote-yggdrasil-ipv6>:8080
	LocalTCP []types.TCPMapping

	// LocalUDP forwards a local UDP port to a remote Yggdrasil address (like ssh -L for UDP).
	// Each entry maps Listen (local host:port) -> Mapped (remote Yggdrasil IPv6:port).
	// Example: listen 127.0.0.1:5353 -> forward to [ygg-ipv6]:53.
	// CLI equivalent: -local-udp 127.0.0.1:5353:<remote-yggdrasil-ipv6>:53
	LocalUDP []types.UDPMapping

	// RemoteTCP exposes a local TCP service to the Yggdrasil network (like ssh -R).
	// Each entry maps Listen (Yggdrasil-side port) -> Mapped (local host:port).
	// Listen address is always the node's own Yggdrasil IPv6; only the port is specified.
	// Mapped defaults to [::1] (IPv6 loopback) if address is omitted.
	// Example: ygg-port 80 -> forward to 127.0.0.1:8080.
	// CLI equivalent: -remote-tcp 80:127.0.0.1:8080
	RemoteTCP []types.TCPMapping

	// RemoteUDP exposes a local UDP service to the Yggdrasil network (like ssh -R for UDP).
	// Each entry maps Listen (Yggdrasil-side port) -> Mapped (local host:port).
	// Listen address is always the node's own Yggdrasil IPv6; only the port is specified.
	// Mapped defaults to [::1] (IPv6 loopback) if address is omitted.
	// Example: ygg-port 53 -> forward to 127.0.0.1:53.
	// CLI equivalent: -remote-udp 53:127.0.0.1:53
	RemoteUDP []types.UDPMapping
}
