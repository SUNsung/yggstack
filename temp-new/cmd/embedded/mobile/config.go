package mobile

import (
	"encoding/json"
	"fmt"
	"net"

	"github.com/yggdrasil-network/yggdrasil-go/src/config"
)

// // // // // // // // // //

// GenerateConfig generates a new Yggdrasil node configuration with a random key pair.
// Returns a JSON string suitable for storing and passing to LoadConfigJSON.
func GenerateConfig() (string, error) {
	nodeCfg := config.GenerateConfig()
	nodeCfg.AdminListen = "none"
	b, err := json.MarshalIndent(nodeCfg, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal config: %w", err)
	}
	return string(b), nil
}

// //

type tcpMappingObj struct {
	Listen *net.TCPAddr
	Mapped *net.TCPAddr
}

type udpMappingObj struct {
	Listen *net.UDPAddr
	Mapped *net.UDPAddr
}

// //

func parseTCPMapping(listenStr, mappedStr string) (tcpMappingObj, error) {
	listenAddr, err := net.ResolveTCPAddr("tcp", listenStr)
	if err != nil {
		return tcpMappingObj{}, fmt.Errorf("invalid listen address %q: %w", listenStr, err)
	}
	mappedAddr, err := net.ResolveTCPAddr("tcp", mappedStr)
	if err != nil {
		return tcpMappingObj{}, fmt.Errorf("invalid mapped address %q: %w", mappedStr, err)
	}
	return tcpMappingObj{Listen: listenAddr, Mapped: mappedAddr}, nil
}

func parseUDPMapping(listenStr, mappedStr string) (udpMappingObj, error) {
	listenAddr, err := net.ResolveUDPAddr("udp", listenStr)
	if err != nil {
		return udpMappingObj{}, fmt.Errorf("invalid listen address %q: %w", listenStr, err)
	}
	mappedAddr, err := net.ResolveUDPAddr("udp", mappedStr)
	if err != nil {
		return udpMappingObj{}, fmt.Errorf("invalid mapped address %q: %w", mappedStr, err)
	}
	return udpMappingObj{Listen: listenAddr, Mapped: mappedAddr}, nil
}

func parseRemoteTCPMapping(port int, localStr string) (tcpMappingObj, error) {
	if port < 1 || port > 65535 {
		return tcpMappingObj{}, fmt.Errorf("port %d out of range 1-65535", port)
	}
	mappedAddr, err := net.ResolveTCPAddr("tcp", localStr)
	if err != nil {
		return tcpMappingObj{}, fmt.Errorf("invalid local address %q: %w", localStr, err)
	}
	return tcpMappingObj{
		Listen: &net.TCPAddr{Port: port},
		Mapped: mappedAddr,
	}, nil
}

func parseRemoteUDPMapping(port int, localStr string) (udpMappingObj, error) {
	if port < 1 || port > 65535 {
		return udpMappingObj{}, fmt.Errorf("port %d out of range 1-65535", port)
	}
	mappedAddr, err := net.ResolveUDPAddr("udp", localStr)
	if err != nil {
		return udpMappingObj{}, fmt.Errorf("invalid local address %q: %w", localStr, err)
	}
	return udpMappingObj{
		Listen: &net.UDPAddr{Port: port},
		Mapped: mappedAddr,
	}, nil
}
