package lowpower

import (
	"github.com/yggdrasil-network/yggstack/mod/activity"
)

// // // // // // // // // //

// NodeControlInterface отделяет менеджер энергосбережения от реализации узла.
type NodeControlInterface interface {
	StopComponents()
	StartComponents(origCfg interface{}) error
	ConnCounter() *activity.CounterObj
	SocksAddr() string
	SocksIsUnix() bool
	SocksReadyCh() <-chan struct{}
}

// ManagerInterface — контракт компонента менеджера энергосбережения.
type ManagerInterface interface {
	Run()
	Stop()
	IsLowPower() bool
	TransitionToFullPower()
	SetOrigConfig(cfg interface{})
	OrigConfig() interface{}
	GetState() int32
}
