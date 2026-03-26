# yggstack

Go-библиотека для встраивания узла Yggdrasil в приложения. Предоставляет стандартные Go-сетевые примитивы (
`DialContext`, `Listen`, `ListenPacket`) поверх userspace TCP/IP стека (gVisor netstack), без необходимости создавать
TUN-интерфейс или получать root-права.

## Архитектура

```mermaid
graph TB
    App[Приложение]

    subgraph yggstack
        Obj[yggstack.Obj]
        SOCKS[SOCKS5-прокси]
        Resolver[Резолвер .pk.ygg / DNS]
    end

subgraph core
CoreObj[core.Obj]
Netstack[netstack — userspace TCP/UDP]
NIC[NIC — мост пакетов]
Multicast[Multicast — mDNS обнаружение]
Admin[Admin — управляющий сокет]
end

subgraph external [Внешние зависимости]
YggCore[yggdrasil-go/core]
gVisor[gVisor netstack]
end

App --> Obj
Obj --> CoreObj
Obj --> SOCKS
SOCKS --> Resolver
SOCKS -->|DialContext|CoreObj
Resolver -->|DialContext для DNS|CoreObj

CoreObj --> Netstack
CoreObj --> Multicast
CoreObj --> Admin
Netstack --> NIC
NIC -->|IPv6 - пакеты|YggCore
Netstack --> gVisor
```

## Путь пакета

Как данные проходят через стек — от приложения до Yggdrasil-сети и обратно:

```mermaid
sequenceDiagram
    participant App as Приложение
    participant NS as Netstack (gVisor)
    participant NIC as NIC (мост)
    participant Ygg as Yggdrasil Core
    Note over App, Ygg: Исходящий пакет (Dial / Write)
    App ->> NS: DialContext("tcp", "[ipv6]:port")
    NS ->> NIC: WritePackets(IPv6-пакет)
    NIC ->> Ygg: ipv6rwc.Write(raw bytes)
    Ygg -->> Ygg: Маршрутизация через оверлейную сеть
    Note over App, Ygg: Входящий пакет (Listen / Read)
    Ygg ->> NIC: ipv6rwc.Read(raw bytes)
    NIC ->> NS: DeliverNetworkPacket(IPv6)
    NS ->> App: net.Conn.Read(data)
```

## Структура модуля

```mermaid
graph LR
subgraph "yggstack (корневой пакет)"
A[Obj — фасад]
B[ConfigObj]
C[SOCKSConfigObj]
end

subgraph "core"
D[Obj — узел Yggdrasil]
E[Interface — контракт]
F[netstackObj — TCP/UDP стек]
G[nicObj — LinkEndpoint]
H[componentObj — lifecycle]
end

subgraph "resolver"
I[Obj — резолвер имён]
end

subgraph "socks"
J[Obj — SOCKS5-сервер]
K[ObjInterface — контракт]
end

A -->|встраивает|E
A -->|использует|J
A -->|создаёт|I
D -->|реализует|E
D -->|содержит|F
F -->|содержит|G
D -->|содержит|H
J -->|реализует|K
```

## Пакеты

### `yggstack` (корневой)

Фасад для встраивания. Объединяет ядро, SOCKS-прокси и резолвер в одну точку входа.

| Тип              | Назначение                                                              |
|------------------|-------------------------------------------------------------------------|
| `Obj`            | Узел с полным набором возможностей: сетевые методы + SOCKS + управление |
| `ConfigObj`      | Контекст, конфиг Yggdrasil, логгер, таймаут                             |
| `SOCKSConfigObj` | Адрес прокси, DNS-сервер, verbose, лимит соединений                     |

### `core`

Ядро — узел Yggdrasil с userspace сетевым стеком.

| Тип            | Назначение                                                                   |
|----------------|------------------------------------------------------------------------------|
| `Obj`          | Узел: DialContext, Listen, ListenPacket, управление пирами, multicast, admin |
| `Interface`    | Публичный контракт — всё, что нужно внешнему коду                            |
| `netstackObj`  | gVisor TCP/UDP/ICMP стек                                                     |
| `nicObj`       | Мост между gVisor и Yggdrasil на уровне IPv6-пакетов                         |
| `componentObj` | Обобщённый Enable/Disable lifecycle для multicast и admin                    |

### `resolver`

Резолвер имён с тремя стратегиями:

```mermaid
flowchart TD
    Input[Входное имя]
    Input --> PK{Суффикс .pk.ygg?}
    PK -->|Да| HEX[Декодировать hex публичного ключа]
    HEX --> ADDR[Вычислить IPv6 из ключа]
    PK -->|Нет| IP{Это IPv6-литерал?}
    IP -->|Да| PASS[Вернуть как есть]
    IP -->|Нет| NS{Nameserver настроен?}
    NS -->|Нет| ERR[Ошибка: no nameserver configured]
    NS -->|Да| DNS[DNS-запрос через Yggdrasil]
    DNS --> RESULT[Первый AAAA-адрес]
```

### `socks`

SOCKS5-прокси поверх Yggdrasil. Поддерживает TCP и Unix-сокеты.

```mermaid
stateDiagram-v2
    [*] --> Created: New()
    Created --> Enabled: Enable()
    Enabled --> Created: Disable()
    Enabled --> Enabled: Enable() → ошибка
    Created --> Created: Disable() → no-op
```

## Жизненный цикл

```mermaid
flowchart TD
    START([Создание]) --> NEW[yggstack.New]
    NEW --> CORE[Запуск Yggdrasil Core]
    CORE --> NS[Создание netstack + NIC]
    NS --> READY([Узел готов])
    READY -->|опционально| SOCKS[EnableSOCKS]
    READY -->|опционально| MC[EnableMulticast]
    READY -->|опционально| ADM[EnableAdmin]
    READY -->|опционально| PEER[AddPeer / RemovePeer]
    SOCKS --> READY
    MC --> READY
    ADM --> READY
    PEER --> READY
    READY --> CLOSE[Close]
    CLOSE --> S1[Disable SOCKS]
    S1 --> S2[Disable Multicast + Admin]
    S2 --> S3[Закрыть listeners]
    S3 --> S4[core.Stop]
    S4 --> S5[Уничтожить netstack]
    S5 --> DONE([Завершено])
```

## Быстрый старт

```go
package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/yggdrasil-network/yggstack/temp-new"
)

func main() {
	// Создаём узел (случайные ключи)
	node, err := yggstack.New(yggstack.ConfigObj{
		Ctx: context.Background(),
	})
	if err != nil {
		panic(err)
	}
	defer node.Close()

	fmt.Println("Адрес:", node.Address())

	// HTTP-клиент через Yggdrasil
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: node.DialContext,
		},
	}

	// Запрос к Yggdrasil-узлу
	resp, err := client.Get("http://[200:abcd::1]:8080/")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
}
```

## Запуск с SOCKS5-прокси

```go
// Включаем SOCKS5
err = node.EnableSOCKS(yggstack.SOCKSConfigObj{
Addr:           "127.0.0.1:1080",
Nameserver:     "[200:abcd::1]:53", // DNS через Yggdrasil
Verbose:        true,
MaxConnections: 128, // 0 = без ограничений
})
if err != nil {
panic(err)
}
defer node.DisableSOCKS()

// Теперь curl --proxy socks5h://127.0.0.1:1080 http://example.pk.ygg/
```

## TCP-сервер в сети Yggdrasil

```go
ln, err := node.Listen("tcp", ":8080")
if err != nil {
panic(err)
}
defer ln.Close()

fmt.Printf("Слушаю на http://[%s]:8080/\n", node.Address())
http.Serve(ln, handler)
```
