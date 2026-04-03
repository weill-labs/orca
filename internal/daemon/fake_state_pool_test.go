package daemon

import (
	"context"
	"errors"
	"sync"
)

type fakeState struct {
	mu                    sync.Mutex
	rejectCanceledContext bool
	tasks                 map[string]Task
	workers               map[string]Worker
	events                []Event
}

func newFakeState() *fakeState {
	return &fakeState{
		tasks:   make(map[string]Task),
		workers: make(map[string]Worker),
	}
}

func (s *fakeState) TaskByIssue(ctx context.Context, project, issue string) (Task, error) {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return Task{}, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.tasks[issue]
	if !ok || task.Project != "" && task.Project != project {
		return Task{}, ErrTaskNotFound
	}
	return task, nil
}

func (s *fakeState) PutTask(ctx context.Context, task Task) error {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[task.Issue] = task
	return nil
}

func (s *fakeState) PutWorker(ctx context.Context, worker Worker) error {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workers[worker.PaneID] = worker
	return nil
}

func (s *fakeState) DeleteWorker(ctx context.Context, project, paneID string) error {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	worker, ok := s.workers[paneID]
	if !ok || worker.Project != "" && worker.Project != project {
		return ErrWorkerNotFound
	}
	delete(s.workers, paneID)
	return nil
}

func (s *fakeState) RecordEvent(ctx context.Context, event Event) error {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
	return nil
}

func (s *fakeState) task(issue string) (Task, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.tasks[issue]
	return task, ok
}

func (s *fakeState) worker(paneID string) (Worker, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	worker, ok := s.workers[paneID]
	return worker, ok
}

type fakePool struct {
	mu                    sync.Mutex
	rejectCanceledContext bool
	acquireStarted        chan struct{}
	acquireRelease        chan struct{}
	acquireCalls          int
	clone                 Clone
	clones                []Clone
	acquired              map[string]bool
	released              []Clone
}

func (p *fakePool) Acquire(ctx context.Context, project, issue string) (Clone, error) {
	if p.rejectCanceledContext && ctx.Err() != nil {
		return Clone{}, ctx.Err()
	}
	p.mu.Lock()
	p.acquireCalls++
	callNumber := p.acquireCalls
	p.mu.Unlock()

	if callNumber == 1 && p.acquireStarted != nil {
		select {
		case p.acquireStarted <- struct{}{}:
		default:
		}
	}
	if callNumber == 1 && p.acquireRelease != nil {
		<-p.acquireRelease
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	for _, clone := range p.availableClones() {
		if p.acquired == nil {
			p.acquired = make(map[string]bool)
		}
		if p.acquired[clone.Path] {
			continue
		}
		p.acquired[clone.Path] = true
		return clone, nil
	}
	return Clone{}, errors.New("clone already acquired")
}

func (p *fakePool) Release(ctx context.Context, project string, clone Clone) error {
	if p.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.acquired != nil {
		delete(p.acquired, clone.Path)
	}
	p.released = append(p.released, clone)
	return nil
}

func (p *fakePool) availableClones() []Clone {
	if len(p.clones) > 0 {
		return p.clones
	}
	return []Clone{p.clone}
}

func (p *fakePool) acquireCallCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.acquireCalls
}

func (p *fakePool) releasedClones() []Clone {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Clone, len(p.released))
	copy(out, p.released)
	return out
}
