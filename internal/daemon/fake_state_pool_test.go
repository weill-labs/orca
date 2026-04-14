package daemon

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type fakeState struct {
	mu                    sync.Mutex
	rejectCanceledContext bool
	tasks                 map[string]Task
	workers               map[string]Worker
	cloneOccupancies      []CloneOccupancy
	mergeQueue            []MergeQueueEntry
	updateMergeErr        error
	deleteMergeErr        error
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
	task.WorkerID = taskWorkerID(task)

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
	previous.WorkerID = taskWorkerID(*previous)
	s.tasks[issue] = *previous
	return nil
}

func (s *fakeState) PutTask(ctx context.Context, task Task) error {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	task.WorkerID = taskWorkerID(task)
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
	if worker.LastSeenAt.IsZero() {
		worker.LastSeenAt = worker.UpdatedAt
	}
	s.workers[workerKey(worker)] = worker
	return nil
}

func (s *fakeState) ClaimWorker(ctx context.Context, worker Worker) (Worker, error) {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return Worker{}, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	var claimed *Worker
	for _, existing := range s.workers {
		if existing.Project != "" && existing.Project != worker.Project {
			continue
		}
		if existing.AgentProfile != worker.AgentProfile || existing.Issue != "" {
			continue
		}
		copy := existing
		if claimed == nil || workerSortLess(copy, *claimed) {
			claimed = &copy
		}
	}

	if claimed == nil {
		if worker.LastSeenAt.IsZero() {
			worker.LastSeenAt = worker.UpdatedAt
		}
		worker.WorkerID = nextFakeWorkerID(s.workers, worker.Project)
		if worker.PaneName == "" {
			worker.PaneName = worker.WorkerID
		}
		s.workers[worker.WorkerID] = worker
		return worker, nil
	}

	claimed.Issue = worker.Issue
	if claimed.PaneName == "" {
		claimed.PaneName = claimed.WorkerID
	}
	s.workers[claimed.WorkerID] = *claimed
	return *claimed, nil
}

func (s *fakeState) WorkerByID(ctx context.Context, project, workerID string) (Worker, error) {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return Worker{}, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	worker, ok := s.workers[workerID]
	if ok && (worker.Project == "" || worker.Project == project) {
		return worker, nil
	}
	for _, worker := range s.workers {
		if worker.Project != "" && worker.Project != project {
			continue
		}
		if worker.PaneID == workerID {
			return worker, nil
		}
	}
	return Worker{}, ErrWorkerNotFound
}

func (s *fakeState) WorkerByPane(ctx context.Context, project, paneID string) (Worker, error) {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return Worker{}, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, worker := range s.workers {
		if worker.Project != "" && worker.Project != project {
			continue
		}
		if worker.PaneID == paneID {
			return worker, nil
		}
	}
	return Worker{}, ErrWorkerNotFound
}

func (s *fakeState) DeleteWorker(ctx context.Context, project, workerID string) error {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if worker, ok := s.workers[workerID]; ok && (worker.Project == "" || worker.Project == project) {
		delete(s.workers, workerID)
		return nil
	}
	for key, worker := range s.workers {
		if worker.Project != "" && worker.Project != project {
			continue
		}
		if worker.PaneID == workerID {
			delete(s.workers, key)
			return nil
		}
	}
	return ErrWorkerNotFound
}

func (s *fakeState) ListWorkers(ctx context.Context, project string) ([]Worker, error) {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	workers := make([]Worker, 0, len(s.workers))
	for _, worker := range s.workers {
		if worker.Project != "" && worker.Project != project {
			continue
		}
		workers = append(workers, worker)
	}
	sort.Slice(workers, func(i, j int) bool { return workerSortLess(workers[i], workers[j]) })
	return workers, nil
}

func (s *fakeState) NonTerminalTasks(ctx context.Context, project string) ([]Task, error) {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	tasks := make([]Task, 0)
	for _, task := range s.tasks {
		if task.Project != "" && task.Project != project {
			continue
		}
		switch task.Status {
		case "", TaskStatusDone, TaskStatusCancelled, TaskStatusFailed:
			continue
		default:
			tasks = append(tasks, task)
		}
	}
	return tasks, nil
}

func (s *fakeState) StaleCloneOccupancies(ctx context.Context, project string) ([]CloneOccupancy, error) {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	occupancies := make([]CloneOccupancy, 0)
	for _, occupancy := range s.cloneOccupancies {
		if project != "" && occupancy.Project != "" && occupancy.Project != project {
			continue
		}
		task, ok := s.tasks[occupancy.AssignedTask]
		if ok && task.Project != "" && occupancy.Project != "" && task.Project != occupancy.Project {
			ok = false
		}
		if ok && taskBlocksAssignment(task.Status) {
			continue
		}
		occupancies = append(occupancies, occupancy)
	}
	return occupancies, nil
}

func (s *fakeState) TasksByPane(ctx context.Context, project, paneID string) ([]Task, error) {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	tasks := make([]Task, 0)
	for _, task := range s.tasks {
		if task.Project != "" && task.Project != project {
			continue
		}
		worker, ok := s.workers[taskWorkerID(task)]
		if !ok {
			if task.PaneID != paneID {
				continue
			}
		} else if worker.PaneID != paneID {
			continue
		}
		tasks = append(tasks, task)
	}

	sort.Slice(tasks, func(i, j int) bool {
		if !tasks[i].CreatedAt.Equal(tasks[j].CreatedAt) {
			return tasks[i].CreatedAt.Before(tasks[j].CreatedAt)
		}
		if !tasks[i].UpdatedAt.Equal(tasks[j].UpdatedAt) {
			return tasks[i].UpdatedAt.Before(tasks[j].UpdatedAt)
		}
		return tasks[i].Issue < tasks[j].Issue
	})

	return tasks, nil
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
		worker, ok := s.workers[taskWorkerID(task)]
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
	worker, ok := s.workers[taskWorkerID(task)]
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
		worker, ok := s.workers[taskWorkerID(task)]
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

func (s *fakeState) MergeEntries(ctx context.Context, project string) ([]MergeQueueEntry, error) {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	entries := make([]MergeQueueEntry, 0, len(s.mergeQueue))
	for _, entry := range s.mergeQueue {
		if entry.Project == project {
			entries = append(entries, entry)
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if !entries[i].CreatedAt.Equal(entries[j].CreatedAt) {
			return entries[i].CreatedAt.Before(entries[j].CreatedAt)
		}
		return entries[i].PRNumber < entries[j].PRNumber
	})
	return entries, nil
}

func (s *fakeState) UpdateMergeEntry(ctx context.Context, entry MergeQueueEntry) error {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.updateMergeErr != nil {
		return s.updateMergeErr
	}
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
	if s.deleteMergeErr != nil {
		return s.deleteMergeErr
	}
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
	task.WorkerID = taskWorkerID(task)
	s.tasks[task.Issue] = task
}

func (s *fakeState) worker(workerID string) (Worker, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	worker, ok := s.workers[workerID]
	if ok {
		return worker, true
	}
	for _, worker := range s.workers {
		if worker.PaneID == workerID {
			return worker, true
		}
	}
	return worker, ok
}

func workerKey(worker Worker) string {
	switch {
	case strings.TrimSpace(worker.WorkerID) != "":
		return strings.TrimSpace(worker.WorkerID)
	default:
		if strings.TrimSpace(worker.PaneID) != "" {
			return strings.TrimSpace(worker.PaneID)
		}
		return strings.TrimSpace(worker.PaneName)
	}
}

func nextFakeWorkerID(workers map[string]Worker, project string) string {
	maxSequence := 0
	for _, worker := range workers {
		if worker.Project != "" && worker.Project != project {
			continue
		}
		if !strings.HasPrefix(worker.WorkerID, "worker-") {
			continue
		}
		sequence, err := strconv.Atoi(strings.TrimPrefix(worker.WorkerID, "worker-"))
		if err != nil {
			continue
		}
		if sequence > maxSequence {
			maxSequence = sequence
		}
	}
	return "worker-" + fmtSequence(maxSequence+1)
}

func fmtSequence(sequence int) string {
	if sequence < 10 {
		return "0" + strconv.Itoa(sequence)
	}
	return strconv.Itoa(sequence)
}

func workerSortLess(left, right Worker) bool {
	leftCreatedAt := left.CreatedAt
	if leftCreatedAt.IsZero() {
		leftCreatedAt = left.UpdatedAt
	}
	rightCreatedAt := right.CreatedAt
	if rightCreatedAt.IsZero() {
		rightCreatedAt = right.UpdatedAt
	}
	switch {
	case !leftCreatedAt.Equal(rightCreatedAt):
		return leftCreatedAt.Before(rightCreatedAt)
	default:
		return left.WorkerID < right.WorkerID
	}
}

func taskWorkerID(task Task) string {
	switch {
	case strings.TrimSpace(task.WorkerID) != "":
		return strings.TrimSpace(task.WorkerID)
	default:
		return strings.TrimSpace(task.PaneID)
	}
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
