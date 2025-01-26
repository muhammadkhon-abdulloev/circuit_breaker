package main

import (
	"context"
	"testing"
	"time"
)

func TestCircuitBreaker_Execute(t *testing.T) {
	cb := NewCB[time.Duration, string](time.Second*3, time.Second*3, 1, 3)

	testCases := []TestCase[time.Duration, string]{
		{
			name:   "Success",
			f:      F,
			params: time.Second * 1,
		},
		{
			name:   "Success",
			f:      F,
			params: time.Second * 1,
		},
		{
			name:   "Success",
			f:      F,
			params: time.Second * 1,
		},
		{
			name:     "Fail_Context_Timeout",
			f:        F,
			params:   time.Second * 10,
			mustFail: true,
		},
		{
			name:     "Fail_Status_Open",
			f:        F,
			mustFail: true,
			sleepFor: time.Second * 1,
		},

		{
			name:     "Fail_Status_Open",
			f:        F,
			params:   time.Second * 1,
			sleepFor: time.Second * 1,
			mustFail: true,
		},

		{
			name:     "Success",
			f:        F,
			params:   time.Second * 1,
			sleepFor: time.Second * 2,
		},

		{
			name:   "Success",
			f:      F,
			params: time.Second * 1,
		},
		{
			name:   "Success",
			f:      F,
			params: time.Second * 1,
		},
	}

	ctx := context.Background()

	for _, testCase := range testCases {
		t.Logf("Running test case: %s", testCase.name)

		time.Sleep(testCase.sleepFor)

		result, err := cb.Execute(ctx, testCase.params, testCase.f)
		if !testCase.mustFail && err != nil {
			t.Errorf("Got error: %s", err.Error())
			continue
		}

		if testCase.mustFail && err == nil {
			t.Errorf("Wanted error, but got nil")
			continue
		} else if testCase.mustFail && err != nil {
			t.Logf("Error: %s", err.Error())
			continue
		}

		t.Logf("Result: %s", result)
	}
}

type TestCase[TRequest, TResponse any] struct {
	name     string
	f        func(ctx context.Context, params TRequest) (TResponse, error)
	params   TRequest
	mustFail bool
	sleepFor time.Duration
}

func F(_ context.Context, sleep time.Duration) (string, error) {
	time.Sleep(sleep)
	return "ok", nil
}
