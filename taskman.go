// Copyright 2015 Bryan Jeal <bryan@jeal.ca>

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// 	http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// taskman is a simple TaskManager that essentially runs functions in the background

package taskman

import (
	"errors"
	"strings"
	"sync"

	"github.com/golang/glog"
	uuid "github.com/satori/go.uuid"
)

// Errors
var (
	ErrInvalidName   = errors.New("name cannot be blank or all spaces")
	ErrAlreadyExists = errors.New("task with that name already exists")
	ErrTaskNotFound  = errors.New("task with supplied name not found")
	ErrNotActive     = errors.New("task manager has been shutdown")
)

// Service is the interface that provides auth methods.
type Service interface {
	// New adds a stored task to the manager
	New(name string, taskFunc TaskRunFunc) error

	// Check takes a task name and checks to see if there are any errors from that task
	Check(name string) []error

	// Run takes a task name and runs it with supplied args
	Run(name string, args ...interface{}) (chan error, error)

	// RunTask takes a Task and queues it to run
	RunTask(task Task) (chan error, error)

	// Shutdown stops the service
	Shutdown() error
}

// TaskRunFunc is the function that is run
type TaskRunFunc func(args ...interface{}) error

// TaskManager is the core of the taskman service
// it is responsible for running tasks
type TaskManager struct {
	*sync.RWMutex
	store  map[string]*Task
	tasks  chan *taskRun
	quit   chan struct{}
	active bool
}

// Task model represents each task in the system
type Task struct {
	Name    string
	Func    TaskRunFunc
	Errs    []error
	ErrChan chan error
	Quit    chan struct{}
}

type taskRun struct {
	*Task
	Args    []interface{}
	inStore bool
}

// NewTaskManager initializes a new TaskMan service and returns the instance of it
func NewTaskManager() (tm *TaskManager) {
	tm = &TaskManager{
		RWMutex: &sync.RWMutex{},
		store:   make(map[string]*Task),
		tasks:   make(chan *taskRun),
		quit:    make(chan struct{}),
		active:  true,
	}

	// start running the service
	go tm.consume()

	return tm
}

// New adds a stored task to the manager
func (tm *TaskManager) New(name string, taskFunc TaskRunFunc) error {
	// make sure task name is valid
	err := checkName(name)
	if err != nil {
		return err
	}

	// make sure taskman is active and
	// check to see if a task with the name already exists
	var nameExists bool
	tm.RLock()
	if !tm.active {
		return ErrNotActive
	}
	_, nameExists = tm.store[name]
	tm.RUnlock()

	if nameExists {
		return ErrAlreadyExists
	}

	// create task
	t := &Task{
		Name: name,
		Func: taskFunc,
		Quit: make(chan struct{}),
	}

	// add task to store
	tm.Lock()
	tm.store[name] = t
	tm.Unlock()

	return nil
}

// Check takes a task name and checks to see if there are any errors from that task
func (tm *TaskManager) Check(name string) []error {
	// make sure task name is valid
	err := checkName(name)
	if err != nil {
		return []error{err}
	}

	tm.RLock()
	t, ok := tm.store[name]
	tm.RUnlock()

	if !ok {
		return []error{ErrTaskNotFound}
	}

	return t.Errs
}

// Run takes a task name and runs it with supplied args
func (tm *TaskManager) Run(name string, args ...interface{}) (chan error, error) {
	// make an errChan to pass along when we fail
	// close it right away to it returns if listened to
	closedErrChan := make(chan error)
	close(closedErrChan)

	// make sure task name is valid
	err := checkName(name)
	if err != nil {
		return closedErrChan, err
	}

	// make sure task manager is active and
	// check to make sure the task exists in the system
	var task Task
	tm.RLock()
	if !tm.active {
		tm.RUnlock()
		return closedErrChan, ErrNotActive
	}
	t, ok := tm.store[name]
	if !ok {
		tm.RUnlock()
		return closedErrChan, ErrTaskNotFound
	}
	// copy task
	task = *t
	tm.RUnlock()

	// make a buffered error channel because the error
	// might happen before we are listening to the returned channel
	task.ErrChan = make(chan error, 1)
	tm.tasks <- &taskRun{
		Task:    &task,
		Args:    args,
		inStore: true,
	}
	return task.ErrChan, nil
}

// RunTask takes a task and runs it
func (tm *TaskManager) RunTask(task Task, args ...interface{}) (chan error, error) {
	// make an errChan to pass along when we fail
	// close it right away to it returns if listened to
	closedErrChan := make(chan error)
	close(closedErrChan)

	// make sure task manager is active
	tm.RLock()
	if !tm.active {
		return closedErrChan, ErrNotActive
	}
	tm.RUnlock()

	// generate name
	task.Name = uuid.NewV4().String()

	// make sure quit isn't nil
	if task.Quit == nil {
		task.Quit = make(chan struct{})
	}

	// add to store
	t := task
	tm.Lock()
	tm.store[task.Name] = &t
	tm.Unlock()

	// make a buffered error channel because the error
	// might happen before we are listening to the returned channel
	task.ErrChan = make(chan error, 1)
	tm.tasks <- &taskRun{
		Task:    &task,
		Args:    args,
		inStore: false,
	}

	return task.ErrChan, nil
}

// Shutdown stops task processing
// sends "quit" to all tasks as well (for long running tasks)
func (tm *TaskManager) Shutdown() {
	tm.Lock()
	for _, t := range tm.store {
		close(t.Quit)
	}
	tm.active = false
	tm.Unlock()
	tm.quit <- struct{}{}

}

func (tm *TaskManager) consume() {
	for {
		var task *taskRun
		select {
		case task = <-tm.tasks:
			// run a task in the background
			go func() {
				err := task.Func(task.Args...)
				task.ErrChan <- err
				if err != nil {
					// log error if there is one
					// we are returning it via the ErrChan,
					// but more likely than not we aren't listening to the returned ErrChan
					glog.Errorln("Task:", task.Name, "had an error:", err)

					// only "collect" errors if task in in the store
					if task.inStore {
						tm.Lock()
						t := *tm.store[task.Name]
						t.Errs = append(t.Errs, err)
						tm.store[task.Name] = &t
						tm.Unlock()
					}
				}
				close(task.ErrChan)
			}()
		case <-tm.quit:
			return
		}
	}
}

func checkName(name string) error {
	name = strings.TrimSpace(name)
	if len(name) == 0 {
		return ErrInvalidName
	}

	return nil
}
