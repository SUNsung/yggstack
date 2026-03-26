package core

import (
	"net"
	"sync"

	yggcore "github.com/yggdrasil-network/yggdrasil-go/src/core"
	"github.com/yggdrasil-network/yggdrasil-go/src/ipv6rwc"

	"gvisor.dev/gvisor/pkg/buffer"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv6"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
)

// // // // // // // // // //

// Проверка реализации интерфейса на этапе компиляции
var _ stack.LinkEndpoint = (*nicObj)(nil)

var writeBufPool = sync.Pool{
	New: func() interface{} { return make([]byte, 65535) },
}

// nicObj — мост между gVisor и Yggdrasil на уровне IPv6-пакетов
type nicObj struct {
	ns         *netstackObj
	ipv6rwc    *ipv6rwc.ReadWriteCloser
	dispatcher stack.NetworkDispatcher
	dispMu     sync.RWMutex
	readBuf    []byte
	rstPackets chan *stack.PacketBuffer
	done       chan struct{}
	readDone   chan struct{}
	rstDone    chan struct{}
	closeOnce  sync.Once
	logger     yggcore.Logger
}

func (s *netstackObj) newNIC(ygg *yggcore.Core) (*nicObj, tcpip.Error) {
	rwc := ipv6rwc.NewReadWriteCloser(ygg)
	nic := &nicObj{
		ns:         s,
		ipv6rwc:    rwc,
		readBuf:    make([]byte, rwc.MTU()),
		rstPackets: make(chan *stack.PacketBuffer, 100),
		done:       make(chan struct{}),
		readDone:   make(chan struct{}),
		rstDone:    make(chan struct{}),
		logger:     s.logger,
	}
	if err := s.stack.CreateNIC(1, nic); err != nil {
		return nil, err
	}

	// Чтение пакетов из Yggdrasil → доставка в netstack
	go func() {
		defer close(nic.readDone)
		for {
			rx, err := nic.ipv6rwc.Read(nic.readBuf)
			if err != nil {
				select {
				case <-nic.done:
				default:
					nic.logger.Println(err)
				}
				return
			}
			pkb := stack.NewPacketBuffer(stack.PacketBufferOptions{
				Payload: buffer.MakeWithData(nic.readBuf[:rx]),
			})
			nic.dispMu.RLock()
			d := nic.dispatcher
			nic.dispMu.RUnlock()
			if d != nil {
				d.DeliverNetworkPacket(ipv6.ProtocolNumber, pkb)
			}
			pkb.DecRef()
		}
	}()

	// Отложенная отправка RST-пакетов (TCP reset без payload)
	go func() {
		defer close(nic.rstDone)
		for {
			select {
			case <-nic.done:
				for {
					select {
					case pkt := <-nic.rstPackets:
						pkt.DecRef()
					default:
						return
					}
				}
			case pkt := <-nic.rstPackets:
				if pkt == nil {
					continue
				}
				_ = nic.writePacket(pkt)
				pkt.DecRef()
			}
		}
	}()

	// Маршрут для Yggdrasil-подсети 0200::/7
	_, snet, err := net.ParseCIDR("0200::/7")
	if err != nil {
		return nil, &tcpip.ErrBadAddress{}
	}
	subnet, err := tcpip.NewSubnet(
		tcpip.AddrFromSlice(snet.IP.To16()),
		tcpip.MaskFrom(string(snet.Mask)),
	)
	if err != nil {
		return nil, &tcpip.ErrBadAddress{}
	}
	s.stack.AddRoute(tcpip.Route{Destination: subnet, NIC: 1})

	// Регистрация локального адреса для HandleLocal
	if s.stack.HandleLocal() {
		ip := ygg.Address()
		if err := s.stack.AddProtocolAddress(
			1,
			tcpip.ProtocolAddress{
				Protocol:          ipv6.ProtocolNumber,
				AddressWithPrefix: tcpip.AddrFromSlice(ip.To16()).WithPrefix(),
			},
			stack.AddressProperties{},
		); err != nil {
			return nil, err
		}
	}

	return nic, nil
}

// //

func (e *nicObj) Attach(dispatcher stack.NetworkDispatcher) {
	e.dispMu.Lock()
	e.dispatcher = dispatcher
	e.dispMu.Unlock()
}

func (e *nicObj) IsAttached() bool {
	e.dispMu.RLock()
	defer e.dispMu.RUnlock()
	return e.dispatcher != nil
}

func (e *nicObj) MTU() uint32                                { return uint32(e.ipv6rwc.MTU()) }
func (e *nicObj) SetMTU(uint32)                              {}
func (*nicObj) Capabilities() stack.LinkEndpointCapabilities { return stack.CapabilityNone }
func (*nicObj) MaxHeaderLength() uint16                      { return 40 }
func (*nicObj) LinkAddress() tcpip.LinkAddress               { return "" }
func (*nicObj) SetLinkAddress(tcpip.LinkAddress)             {}
func (*nicObj) Wait()                                        {}

// //

func (e *nicObj) writePacket(pkt *stack.PacketBuffer) tcpip.Error {
	defer func() {
		if r := recover(); r != nil {
			e.logger.Println("writePacket panic:", r)
		}
	}()
	buf := writeBufPool.Get().([]byte)
	defer writeBufPool.Put(buf)
	vv := pkt.ToView()
	n, err := vv.Read(buf)
	if err != nil {
		return &tcpip.ErrAborted{}
	}
	if _, err = e.ipv6rwc.Write(buf[:n]); err != nil {
		return &tcpip.ErrAborted{}
	}
	return nil
}

func (e *nicObj) WritePackets(list stack.PacketBufferList) (int, tcpip.Error) {
	for i, pkt := range list.AsSlice() {
		// TCP RST без payload — отправляем отложенно через канал
		if pkt.Data().Size() == 0 &&
			pkt.Network().TransportProtocol() == tcp.ProtocolNumber {
			tcpHdr := header.TCP(pkt.TransportHeader().Slice())
			if (tcpHdr.Flags() & header.TCPFlagRst) == header.TCPFlagRst {
				pkt.IncRef()
				select {
				case e.rstPackets <- pkt:
				default:
					pkt.DecRef()
				}
				continue
			}
		}
		if err := e.writePacket(pkt); err != nil {
			return i, err
		}
	}
	return list.Len(), nil
}

func (e *nicObj) WriteRawPacket(*stack.PacketBuffer) tcpip.Error {
	return &tcpip.ErrNotSupported{}
}

// //

func (*nicObj) ARPHardwareType() header.ARPHardwareType { return header.ARPHardwareNone }
func (*nicObj) AddHeader(*stack.PacketBuffer)           {}
func (*nicObj) ParseHeader(*stack.PacketBuffer) bool    { return true }

func (e *nicObj) Close() {
	e.closeOnce.Do(func() {
		close(e.done)
		_ = e.ipv6rwc.Close()
		<-e.readDone
		<-e.rstDone
		e.ns.stack.RemoveNIC(1)
	})
}

func (e *nicObj) SetOnCloseAction(func()) {}
