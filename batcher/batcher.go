//go:build !solution

package batcher

import (
	"sync"

	"gitlab.com/slon/shad-go/batcher/slow"
)

type loadResult struct {
	val   any
	gen   uint64
	valid bool
}

type Batcher struct {
	v *slow.Value

	mu       sync.Mutex
	loading  bool
	loadGen  uint64
	waiters  []chan loadResult
	loadDone chan struct{}
}

func NewBatcher(v *slow.Value) *Batcher {
	return &Batcher{
		v:        v,
		loadDone: make(chan struct{}),
	}
}

func (b *Batcher) Load() any {
	for {
		myGen := b.v.Generation()

		b.mu.Lock()
		if b.loading && b.loadGen == myGen {
			ch := make(chan loadResult, 1)
			b.waiters = append(b.waiters, ch)
			b.mu.Unlock()

			res := <-ch
			if res.valid && res.gen == myGen {
				return res.val
			}
			continue
		}

		if b.loading {
			done := b.loadDone
			b.mu.Unlock()
			<-done
			continue
		}

		b.loading = true
		b.loadGen = myGen
		b.loadDone = make(chan struct{})
		b.mu.Unlock()

		val := b.v.Load()
		valid := b.v.Generation() == myGen
		res := loadResult{val: val, gen: myGen, valid: valid}

		b.mu.Lock()
		waiters := b.waiters
		b.waiters = nil
		b.loading = false
		done := b.loadDone
		b.mu.Unlock()

		close(done)
		for _, ch := range waiters {
			ch <- res
		}

		if valid {
			return val
		}
	}
}
