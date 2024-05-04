package logger

import "fmt"

type Logger interface {
	Infof(msg string, args ...interface{})
	Errorln(msg string, args ...interface{})
}

type DefaultLogger struct {
	id   int
	name string
}

func NewLogger(id int, name string) *DefaultLogger {
	return &DefaultLogger{
		id:   id,
		name: name,
	}
}

func (l *DefaultLogger) Infof(msg string, args ...interface{}) {
	fmt.Printf("[%d::%s] INFO "+msg, append([]interface{}{l.id, l.name}, args...)...)
}

func (l *DefaultLogger) Errorln(msg string, args ...interface{}) {
	fmt.Printf("[%d::%s] ERROR "+msg+"\n", append([]interface{}{l.id, l.name}, args...)...)
}
