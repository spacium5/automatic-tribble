//go:build !solution

package pubsub

import (
	"context"
	"errors"
	"sync"
)

var errClosed = errors.New("pubsub is closed")

var _ Subscription = (*MySubscription)(nil)

type MySubscription struct {
	ps   *MyPubSub
	subj string
	ch   chan interface{}

	unsubOnce   sync.Once
	handlerDone chan struct{}
}

func (s *MySubscription) Unsubscribe() {
	s.unsubOnce.Do(func() {
		s.ps.removeSub(s.subj, s)
		close(s.ch)
	})
}

func (s *MySubscription) waitHandler() {
	<-s.handlerDone
}

var _ PubSub = (*MyPubSub)(nil)

type MyPubSub struct {
	mu     sync.RWMutex
	closed bool
	topics map[string]map[*MySubscription]struct{}

	closeOnce sync.Once
}

func NewPubSub() PubSub {
	return &MyPubSub{
		topics: make(map[string]map[*MySubscription]struct{}),
	}
}

func (p *MyPubSub) Subscribe(subj string, cb MsgHandler) (Subscription, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, errClosed
	}

	s := &MySubscription{
		ps:          p,
		subj:        subj,
		ch:          make(chan interface{}, 1024),
		handlerDone: make(chan struct{}),
	}

	if p.topics[subj] == nil {
		p.topics[subj] = make(map[*MySubscription]struct{})
	}
	p.topics[subj][s] = struct{}{}

	go func() {
		defer close(s.handlerDone)
		for msg := range s.ch {
			cb(msg)
		}
	}()

	return s, nil
}

func (p *MyPubSub) removeSub(subj string, s *MySubscription) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if subs, ok := p.topics[subj]; ok {
		delete(subs, s)
		if len(subs) == 0 {
			delete(p.topics, subj)
		}
	}
}

func (p *MyPubSub) Publish(subj string, msg interface{}) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return errClosed
	}

	subs := make([]*MySubscription, 0, len(p.topics[subj]))
	for s := range p.topics[subj] {
		subs = append(subs, s)
	}
	p.mu.RUnlock()

	for _, s := range subs {
		s.ch <- msg
	}

	return nil
}

func (p *MyPubSub) Close(ctx context.Context) error {
	var wait sync.WaitGroup

	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true

		for _, subs := range p.topics {
			for s := range subs {
				wait.Add(1)
				go func(sub *MySubscription) {
					defer wait.Done()
					sub.Unsubscribe()
					sub.waitHandler()
				}(s)
			}
		}
		p.topics = nil
		p.mu.Unlock()
	})

	done := make(chan struct{})
	go func() {
		wait.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
