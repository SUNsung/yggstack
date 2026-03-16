package lowpower

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yggdrasil-network/yggdrasil-go/src/core"
)

// // // // // // // // // //

// ManagerObj управляет автоматическими переходами узла в спящий/активный режим.
// Все переходы состояний выполняются через Run() — единственную владеющую горутину.
// Внешние вызовы запрашивают пробуждение через TransitionToFullPower, отправляя в wakeCh.
type ManagerObj struct {
	node   NodeControlInterface
	cfg    ConfigObj
	state  atomic.Int32
	ctx    context.Context
	cancel context.CancelFunc
	logger core.Logger

	wakeCh chan struct{} // буфер(1): запрос пробуждения из любой горутины без блокировки

	origCfgMu sync.RWMutex
	origCfg   interface{} // непрозрачная конфигурация, передаваемая в StartComponents при пробуждении

	wakeTrigger      wakeTriggerObj
	socksWaitTimeout time.Duration // таймаут готовности SOCKS при пробуждении (по умолчанию 10 с)

	lastActivityAt atomic.Int64 // Unix-время последней активности
}

const defaultIdleTimeout = 60 * time.Second

// //

func NewManager(node NodeControlInterface, cfg ConfigObj, ctx context.Context, logger core.Logger) *ManagerObj {
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = defaultIdleTimeout
	}
	ctx, cancel := context.WithCancel(ctx)
	m := &ManagerObj{
		node:             node,
		cfg:              cfg,
		ctx:              ctx,
		cancel:           cancel,
		logger:           logger,
		wakeCh:           make(chan struct{}, 1),
		socksWaitTimeout: wakeSOCKSWaitTimeout,
	}
	m.state.Store(StateFullPower)
	m.touchActivity()
	return m
}

func (m *ManagerObj) touchActivity() {
	m.lastActivityAt.Store(time.Now().Unix())
}

func (m *ManagerObj) idleSeconds() int64 {
	return time.Now().Unix() - m.lastActivityAt.Load()
}

// //

// Run — единственный владелец переходов состояний.
// Должен выполняться в отдельной горутине.
func (m *ManagerObj) Run() {
	ticker := time.NewTicker(idleCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return

		case <-m.wakeCh:
			// Запрос пробуждения от TransitionToFullPower — обрабатывается вне зависимости от состояния.
			if m.state.Load() != StateFullPower {
				m.doTransitionToFullPower()
			}

		case <-ticker.C:
			if m.state.Load() != StateFullPower {
				continue
			}
			// Обновляет lastActivity при наличии активных соединений.
			if m.node.ConnCounter().Count() > 0 {
				m.touchActivity()
				continue
			}
			if time.Duration(m.idleSeconds())*time.Second >= m.cfg.IdleTimeout {
				m.transitionToLowPower()
				// Обрабатывает сигнал пробуждения, пришедший в процессе остановки.
				select {
				case <-m.wakeCh:
					m.doTransitionToFullPower()
				default:
				}
			}
		}
	}
}

// //

// transitionToLowPower останавливает компоненты и переводит в спящий режим.
// Вызывается только из Run(). Не выполняет действий, если не в StateFullPower.
func (m *ManagerObj) transitionToLowPower() {
	if m.state.Load() != StateFullPower {
		return
	}
	m.state.Store(StateStopping)
	m.logger.Infof("Low power mode: entering sleep after %s idle", m.cfg.IdleTimeout)

	socksAddr := m.node.SocksAddr()

	m.node.StopComponents()

	// Триггер пробуждения на порту SOCKS: будит узел при подключении клиента.
	if socksAddr != "" {
		if err := m.startWakeTrigger(socksAddr); err != nil {
			m.logger.Errorf("Low power mode: %s — restarting components", err)
			if startErr := m.node.StartComponents(m.getOrigCfg()); startErr != nil {
				m.logger.Errorf("Low power mode: failed to restart after wake trigger failure: %s", startErr)
				m.state.Store(StateLowPower)
				return
			}
			m.touchActivity()
			m.state.Store(StateFullPower)
			return
		}
	}

	m.state.Store(StateLowPower)
	m.logger.Infof("Low power mode: sleeping")
}

// doTransitionToFullPower будит узел.
// Вызывается только из Run().
func (m *ManagerObj) doTransitionToFullPower() {
	m.state.Store(StateStarting)
	m.logger.Infof("Low power mode: waking up")

	// Закрывает слушатель — новые соединения не принимаются,
	// но горутины handleWakeConnection продолжают работу и ожидают SOCKS.
	m.closeWakeListener()

	if err := m.node.StartComponents(m.getOrigCfg()); err != nil {
		m.logger.Errorf("Low power mode: failed to restart components: %s", err)
		// Повторно включает триггер пробуждения для будущих попыток подключения.
		if socksAddr := m.node.SocksAddr(); socksAddr != "" {
			if triggerErr := m.startWakeTrigger(socksAddr); triggerErr != nil {
				m.logger.Errorf("Low power mode: failed to re-enable wake trigger: %s", triggerErr)
			}
		}
		m.state.Store(StateLowPower)
		return
	}

	m.touchActivity()
	m.state.Store(StateFullPower)
	m.logger.Infof("Low power mode: fully awake")
}

// //

// TransitionToFullPower сигнализирует Run() о пробуждении узла.
// Неблокирующий: если пробуждение уже поставлено в очередь, вызов — холостой.
// Безопасен для вызова из любой горутины.
func (m *ManagerObj) TransitionToFullPower() {
	select {
	case m.wakeCh <- struct{}{}:
	default:
	}
}

// //

// IsLowPower возвращает true, если узел спит или переходит в спящий режим.
func (m *ManagerObj) IsLowPower() bool {
	s := m.state.Load()
	return s == StateLowPower || s == StateStopping
}

// SetOrigConfig сохраняет конфигурацию для перезапуска при пробуждении.
// Безопасен для вызова из любой горутины.
func (m *ManagerObj) SetOrigConfig(cfg interface{}) {
	m.origCfgMu.Lock()
	m.origCfg = cfg
	m.origCfgMu.Unlock()
}

// OrigConfig возвращает сохранённую конфигурацию перезапуска.
func (m *ManagerObj) OrigConfig() interface{} {
	return m.getOrigCfg()
}

func (m *ManagerObj) getOrigCfg() interface{} {
	m.origCfgMu.RLock()
	defer m.origCfgMu.RUnlock()
	return m.origCfg
}

// GetState возвращает текущее состояние энергосбережения как int32.
func (m *ManagerObj) GetState() int32 {
	return m.state.Load()
}

func (m *ManagerObj) Stop() {
	m.cancel()
	m.stopWakeTrigger()
}
