package activity

// // // // // // // // // //

// CallbackInterface получает уведомления о жизненном цикле соединений.
// Реализации не должны блокироваться.
type CallbackInterface interface {
	OnConnectionCreated(connId string, protocol string)
	OnConnectionClosed(connId string)
}
