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

package taskman

import (
	"errors"
	"testing"
	"time"

	"fmt"

	"github.com/golang/glog"
	"github.com/stretchr/testify/assert"
)

// test cases

var nilFunc = func(args ...interface{}) error { return nil }
var printNameFunc = func(args ...interface{}) error { glog.Infoln(args[0].(string), "Running"); return nil }

var errStdError = errors.New("errStdError")

type expectedErrs struct {
	new     error
	run     error
	errChan error
	runTask error
}

var cases = []struct {
	name     string
	function func(args ...interface{}) error
	args     interface{}
	expected expectedErrs
}{
	{"Valid", printNameFunc, "Valid", expectedErrs{}},
	{"Valid", printNameFunc, "Valid Again!", expectedErrs{
		new: ErrAlreadyExists,
	}},
	{"", nilFunc, nil, expectedErrs{
		new: ErrInvalidName,
		run: ErrInvalidName,
	}},
	{"   ", nilFunc, nil, expectedErrs{
		new: ErrInvalidName,
		run: ErrInvalidName,
	}},
	{" Also Valid ", printNameFunc, " Also Valid ", expectedErrs{}},
	{"Always Error", func(args ...interface{}) error { glog.Infoln("Always Error", "Running"); return errStdError }, nil, expectedErrs{
		errChan: errStdError,
	}},
	{"Run Long", func(args ...interface{}) error { time.Sleep(100 * time.Millisecond); return nil }, nil, expectedErrs{}},
}

func TestTaskMan(t *testing.T) {
	// init
	assert := assert.New(t)
	taskman := NewTaskManager()

	// test: New
	for _, c := range cases {
		err := taskman.New(c.name, c.function)
		assert.Equal(c.expected.new, err)
	}

	// test: Run
	for _, c := range cases {
		errChan, err := taskman.Run(c.name, c.args)
		assert.Equal(c.expected.run, err)
		assert.Equal(c.expected.errChan, <-errChan)
	}
	errChan, err := taskman.Run("Task Not In Store", nil)
	assert.Equal(ErrTaskNotFound, err)
	assert.Equal(nil, <-errChan)

	// test: RunTask
	for _, c := range cases {
		t := Task{
			Func: c.function,
		}
		_, errChan, err := taskman.RunTask(t, c.args)
		assert.Equal(c.expected.runTask, err)
		assert.Equal(c.expected.errChan, <-errChan)
	}

	// test: RunTaskShutdown
	// run forever
	quit := make(chan struct{})
	task := Task{
		Func: func(args ...interface{}) error {
			count := 0
			for {
				select {
				case <-quit:
					fmt.Println("Run Forever. Quit Activated.")
					return nil
				default:
					fmt.Println("Run Forever. Loop:", count)
					count++
					time.Sleep(100 * time.Millisecond)
				}
			}
		},
		Quit: quit,
	}

	_, _, err = taskman.RunTask(task, nil)
	assert.Equal(nil, err)
	time.Sleep(500 * time.Millisecond)
	taskman.Shutdown()

	// test: Shutdown/Active errors
	err = taskman.New("A New Task", nilFunc)
	assert.Equal(ErrNotActive, err)
	_, err = taskman.Run(cases[0].name, cases[0].args)
	assert.Equal(ErrNotActive, err)
	_, _, err = taskman.RunTask(task, nil)
	assert.Equal(ErrNotActive, err)

	// test: Check
	errs := taskman.Check("Always Error")
	assert.Equal(errs, []error{errStdError})
	errs = taskman.Check("")
	assert.Equal(errs, []error{ErrInvalidName})
	errs = taskman.Check("Task Not In Store")
	assert.Equal(errs, []error{ErrTaskNotFound})
	time.Sleep(250 * time.Millisecond)
}
