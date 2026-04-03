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
	mergeQueue            []MergeQueueEntry
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

func (s *fakeState) ClaimTask(ctx context.Context, task Task) (*Task, error) {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.tasks[task.Issue]
	if ok && (existing.Project == "" || existing.Project == task.Project) && taskBlocksAssignment(existing.Status) {
		return nil, errors.New("task already assigned")
	}

	s.tasks[task.Issue] = task
	if !ok {
		return nil, nil
	}

	previous := existing
	return &previous, nil
}

func (s *fakeState) RestoreTask(ctx context.Context, project, issue string, previous *Task) error {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if previous == nil {
		delete(s.tasks, issue)
		return nil
	}
	s.tasks[issue] = *previous
	return nil
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

func (s *fakeState) DeleteTask(ctx context.Context, project, issue string) error {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tasks[issue]; !ok {
		return ErrTaskNotFound
	}
	delete(s.tasks, issue)
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

func (s *fakeState) ActiveAssignments(ctx context.Context, project string) ([]ActiveAssignment, error) {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	assignments := make([]ActiveAssignment, 0)
	for _, task := range s.tasks {
		if task.Project != "" && task.Project != project {
			continue
		}
		if task.Status != TaskStatusActive {
			continue
		}
		worker, ok := s.workers[task.PaneID]
		if !ok {
			continue
		}
		assignments = append(assignments, ActiveAssignment{Task: task, Worker: worker})
	}
	return assignments, nil
}

func (s *fakeState) ActiveAssignmentByIssue(ctx context.Context, project, issue string) (ActiveAssignment, error) {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return ActiveAssignment{}, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	task, ok := s.tasks[issue]
	if !ok || (task.Project != "" && task.Project != project) || task.Status != TaskStatusActive {
		return ActiveAssignment{}, ErrTaskNotFound
	}
	worker, ok := s.workers[task.PaneID]
	if !ok {
		return ActiveAssignment{}, ErrTaskNotFound
	}
	return ActiveAssignment{Task: task, Worker: worker}, nil
}

func (s *fakeState) ActiveAssignmentByPRNumber(ctx context.Context, project string, prNumber int) (ActiveAssignment, error) {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return ActiveAssignment{}, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, task := range s.tasks {
		if task.Project != "" && task.Project != project {
			continue
		}
		if task.Status != TaskStatusActive || task.PRNumber != prNumber {
			continue
		}
		worker, ok := s.workers[task.PaneID]
		if !ok {
			break
		}
		return ActiveAssignment{Task: task, Worker: worker}, nil
	}
	return ActiveAssignment{}, ErrTaskNotFound
}

func (s *fakeState) EnqueueMerge(ctx context.Context, entry MergeQueueEntry) (int, error) {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return 0, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.mergeQueue {
		if existing.Project == entry.Project && existing.PRNumber == entry.PRNumber {
			return 0, errors.New("merge already queued")
		}
	}
	s.mergeQueue = append(s.mergeQueue, entry)
	return len(s.mergeQueue), nil
}

func (s *fakeState) MergeEntry(ctx context.Context, project string, prNumber int) (*MergeQueueEntry, error) {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, entry := range s.mergeQueue {
		if entry.Project == project && entry.PRNumber == prNumber {
			copied := entry
			return &copied, nil
		}
	}
	return nil, nil
}

func (s *fakeState) NextMergeEntry(ctx context.Context, project string) (*MergeQueueEntry, error) {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, entry := range s.mergeQueue {
		if entry.Project == project {
			copied := entry
			return &copied, nil
		}
	}
	return nil, nil
}

func (s *fakeState) UpdateMergeEntry(ctx context.Context, entry MergeQueueEntry) error {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, existing := range s.mergeQueue {
		if existing.Project == entry.Project && existing.PRNumber == entry.PRNumber {
			s.mergeQueue[i] = entry
			return nil
		}
	}
	return ErrTaskNotFound
}

func (s *fakeState) DeleteMergeEntry(ctx context.Context, project string, prNumber int) error {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, entry := range s.mergeQueue {
		if entry.Project == project && entry.PRNumber == prNumber {
			s.mergeQueue = append(s.mergeQueue[:i], s.mergeQueue[i+1:]...)
			return nil
		}
	}
	return ErrTaskNotFound
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

func (s *fakeState) putTaskForTest(task Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[task.Issue] = task
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
