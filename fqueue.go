package fqueue

import "errors"

type Queue interface {
	IsEmpty() bool
	Size() int
	Pop() []byte
	Push(data []byte) error
	Close() error
}

var (
	ErrInvalidQueue   = errors.New("invalid queue")
	ErrNotEnoughSpace = errors.New("not enough space")
)
