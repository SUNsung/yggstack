package socks

// // // // // // // // // //

// ObjInterface — контракт SOCKS5-сервера
type ObjInterface interface {
	Enable(cfg EnableConfigObj) error
	Disable() error
}
