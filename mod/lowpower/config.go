package lowpower

import (
	"time"
)

// // // // // // // // // //

type ConfigObj struct {
	// IdleTimeout — период простоя перед переходом в спящий режим. По умолчанию: 60 с.
	IdleTimeout time.Duration
}
