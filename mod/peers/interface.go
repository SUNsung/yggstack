package peers

import (
	"net/url"

	"github.com/yggdrasil-network/yggdrasil-go/src/core"
)

// // // // // // // // // //

// CoreInterface абстрагирует методы ядра Yggdrasil, используемые пакетом peers.
type CoreInterface interface {
	GetPeers() []core.PeerInfo
	AddPeer(u *url.URL, sintf string) error
	RemovePeer(u *url.URL, sintf string) error
	RetryPeersNow()
}

// ChangeCallbackInterface уведомляется при изменении числа подключённых пиров.
// Реализации не должны блокироваться.
type ChangeCallbackInterface interface {
	OnPeerCountChanged(connected int64, total int64)
}

// MonitorInterface — контракт компонента мониторинга пиров.
type MonitorInterface interface {
	Run()
	Cancel()
}
