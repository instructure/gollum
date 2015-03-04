package Log

import (
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
)

type metrics struct {
	mutex *sync.Mutex
	store map[string]*int64
}

// Metric allows any part of gollum to store and/or modify metric values by
// name.
var Metric = metrics{new(sync.Mutex), make(map[string]*int64)}

// New creates a new metric under the given name with a value of 0
func (met *metrics) New(name string) {
	met.store[name] = new(int64)
}

// Set sets a given metric to a given value. This operation is atomic.
func (met *metrics) Set(name string, value int64) {
	atomic.StoreInt64(met.store[name], value)
}

// SetI is Set for int values (conversion to int64)
func (met *metrics) SetI(name string, value int) {
	atomic.StoreInt64(met.store[name], int64(value))
}

// SetF is Set for float64 values (conversion to int64)
func (met *metrics) SetF(name string, value float64) {
	atomic.StoreInt64(met.store[name], int64(value))
}

// Add adds a number to a given metric. This operation is atomic.
func (met *metrics) Add(name string, value int64) {
	atomic.AddInt64(met.store[name], value)
}

// Sub subtracts a number to a given metric. This operation is atomic.
func (met *metrics) Sub(name string, value int64) {
	atomic.AddInt64(met.store[name], -value)
}

// Get returns the value of a given metric. This operation is atomic.
// If the value does not exists error is non-nil and the returned value is 0.
func (met *metrics) Get(name string) (int64, error) {
	val, exists := met.store[name]
	if !exists {
		return 0, errors.New("Metric " + name + " not found.")
	}
	return atomic.LoadInt64(val), nil
}

func (met *metrics) Dump() ([]byte, error) {
	return json.Marshal(Metric.store)
}
