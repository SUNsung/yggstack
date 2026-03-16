package peers

import (
	"fmt"
	"net/url"
)

// // // // // // // // // //

// AddPeer добавляет постоянный пир во время работы. URI: "tcp://host:port", "quic://host:port" и т.д.
func AddPeer(c CoreInterface, uri string) error {
	if c == nil {
		return fmt.Errorf("node is not running")
	}
	u, err := url.Parse(uri)
	if err != nil {
		return fmt.Errorf("url.Parse: %w", err)
	}
	return c.AddPeer(u, "")
}

// RemovePeer удаляет постоянный пир во время работы.
func RemovePeer(c CoreInterface, uri string) error {
	if c == nil {
		return fmt.Errorf("node is not running")
	}
	u, err := url.Parse(uri)
	if err != nil {
		return fmt.Errorf("url.Parse: %w", err)
	}
	return c.RemovePeer(u, "")
}

// RetryPeersNow принудительно выполняет немедленную попытку переподключения ко всем отключённым пирам.
func RetryPeersNow(c CoreInterface) {
	if c == nil {
		return
	}
	c.RetryPeersNow()
}
