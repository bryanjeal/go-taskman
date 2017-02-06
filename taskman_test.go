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

var errStdError = errors.New("Error!")

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
	{"Run Long", func(args ...interface{}) error { time.Sleep(1 * time.Second); return nil }, nil, expectedErrs{}},
}

func TestTaskMan(t *testing.T) {
	// init
	taskman := NewTaskManager()

	// run tests
	t.Run("New", func(t *testing.T) {
		assert := assert.New(t)

		for _, c := range cases {
			err := taskman.New(c.name, c.function)
			assert.Equal(c.expected.new, err)
		}
	})

	t.Run("Run", func(t *testing.T) {
		assert := assert.New(t)

		for _, c := range cases {
			errChan, err := taskman.Run(c.name, c.args)
			assert.Equal(c.expected.run, err)
			assert.Equal(c.expected.errChan, <-errChan)
		}
	})

	t.Run("RunTask", func(t *testing.T) {
		assert := assert.New(t)
		for _, c := range cases {
			t := Task{
				Func: c.function,
			}
			errChan, err := taskman.RunTask(t, c.args)
			assert.Equal(c.expected.runTask, err)
			assert.Equal(c.expected.errChan, <-errChan)
		}
	})

	t.Run("RunTaskShutdown", func(t *testing.T) {
		assert := assert.New(t)

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
						time.Sleep(250 * time.Millisecond)
					}
				}
			},
			Quit: quit,
		}

		_, err := taskman.RunTask(task, nil)
		assert.Equal(nil, err)
		time.Sleep(1 * time.Second)
		taskman.Shutdown()

		_, err = taskman.Run(cases[0].name, cases[0].args)
		assert.Equal(ErrNotActive, err)
		time.Sleep(1 * time.Second)
	})
}
