package core

import (
	"context"
	"fmt"
	"net"
	"strconv"

	yggcore "github.com/yggdrasil-network/yggdrasil-go/src/core"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv6"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/icmp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/udp"
)

// // // // // // // // // //

// netstackObj — userspace TCP/UDP стек поверх gVisor
type netstackObj struct {
	stack  *stack.Stack
	nic    *nicObj
	logger yggcore.Logger
}

func newNetstack(ygg *yggcore.Core, log yggcore.Logger) (*netstackObj, error) {
	s := &netstackObj{
		stack: stack.New(stack.Options{
			NetworkProtocols:   []stack.NetworkProtocolFactory{ipv6.NewProtocol},
			TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol, udp.NewProtocol, icmp.NewProtocol6},
			HandleLocal:        true,
		}),
		logger: log,
	}
	if s.stack.HandleLocal() {
		s.stack.AllowICMPMessage()
	} else if err := s.stack.SetForwardingDefaultAndAllNICs(ipv6.ProtocolNumber, true); err != nil {
		return nil, fmt.Errorf("SetForwardingDefaultAndAllNICs: %s", err.String())
	}
	nic, tcpErr := s.newNIC(ygg)
	if tcpErr != nil {
		return nil, fmt.Errorf("newNIC: %s", tcpErr.String())
	}
	s.nic = nic
	return s, nil
}

func (s *netstackObj) close() {
	if s.nic != nil {
		s.nic.Close()
	}
	s.stack.Destroy()
}

// //

func parseAddress(address string) (tcpip.FullAddress, tcpip.NetworkProtocolNumber, error) {
	host, portStr, err := net.SplitHostPort(address)
	if err != nil {
		return tcpip.FullAddress{}, 0, fmt.Errorf("net.SplitHostPort: %w", err)
	}
	port := 80
	if portStr != "" {
		port, err = strconv.Atoi(portStr)
		if err != nil {
			return tcpip.FullAddress{}, 0, fmt.Errorf("strconv.Atoi: %w", err)
		}
	}
	addr := tcpip.Address{}
	if ip := net.ParseIP(host); ip != nil {
		addr = tcpip.AddrFromSlice(ip.To16())
	}
	return tcpip.FullAddress{NIC: 1, Addr: addr, Port: uint16(port)}, ipv6.ProtocolNumber, nil
}

// //

// DialContext — стандартный Go dialer; tcp, tcp6, udp, udp6
func (s *netstackObj) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	fa, pn, err := parseAddress(address)
	if err != nil {
		return nil, err
	}
	switch network {
	case "tcp", "tcp6":
		return gonet.DialContextTCP(ctx, s.stack, fa, pn)
	case "udp", "udp6":
		return gonet.DialUDP(s.stack, nil, &fa, pn)
	default:
		return nil, fmt.Errorf("unsupported network %q", network)
	}
}

// Listen — TCP listener; tcp, tcp6
func (s *netstackObj) Listen(network, address string) (net.Listener, error) {
	fa, pn, err := parseAddress(address)
	if err != nil {
		return nil, err
	}
	switch network {
	case "tcp", "tcp6":
		return gonet.ListenTCP(s.stack, fa, pn)
	default:
		return nil, fmt.Errorf("unsupported network %q for Listen", network)
	}
}

// ListenPacket — UDP listener; udp, udp6
func (s *netstackObj) ListenPacket(network, address string) (net.PacketConn, error) {
	fa, pn, err := parseAddress(address)
	if err != nil {
		return nil, err
	}
	switch network {
	case "udp", "udp6":
		return gonet.DialUDP(s.stack, &fa, nil, pn)
	default:
		return nil, fmt.Errorf("unsupported network %q for ListenPacket", network)
	}
}

// MTU — MTU сетевого интерфейса
func (s *netstackObj) MTU() uint64 {
	if s.nic == nil {
		return 0
	}
	return uint64(s.nic.MTU())
}
