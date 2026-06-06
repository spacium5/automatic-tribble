//go:build !change

package slow

import (
	"sync"
	"sync/atomic"
	"time"
)

type Value struct {
	mu          sync.Mutex
	value       any
	readRunning int32
	storeGen    atomic.Uint64
}

func (s *Value) Load() any {
	if atomic.SwapInt32(&s.readRunning, 1) == 1 {
		panic("another load is running")
	}
	defer atomic.StoreInt32(&s.readRunning, 0)

	s.mu.Lock()
	value := s.value
	s.mu.Unlock()

	time.Sleep(time.Millisecond)
	return value
}

func (s *Value) Store(v interface{}) {
	s.mu.Lock()
	s.value = v
	s.mu.Unlock()

	s.storeGen.Add(1)
}

func (s *Value) Generation() uint64 {
	return s.storeGen.Load()
}
