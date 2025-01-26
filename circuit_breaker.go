package main

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrCircuitOpened = errors.New("circuit status opened")
)

type Status int

const (
	StatusClosed Status = iota
	StatusOpen
	StatusHalfOpen
)

type CircuitBreaker[TRequest, TResponse any] struct {
	mx *sync.Mutex
	// timeout - сколько секунд будет отрабатывать запрос
	timeout time.Duration
	// recoverTimeout - сколько секунд предохранитель будет в статусе opened
	recoverTimeout time.Duration
	// status - текущий статус нашего предохранителя
	status Status
	// errorThreshold - при каком количестве зафейленных запросов переключаться на статус opened
	errorThreshold float64
	// halfOpenLimit - лимит запросов в статусе halfOpen после чего статус предохранителя перейдет на другой
	halfOpenLimit int64
	// errorResponses - количество зафейленных запросов
	errorResponses int64
	// successResponses - количество успешных запросов
	successResponses int64
	// totalResponses - общее количество запросов
	totalResponses int64
}

func NewCB[TRequest, TResponse any](timeout, recoverTimeout time.Duration, errorThreshold float64, halfOpenLimit int64) *CircuitBreaker[TRequest, TResponse] {
	return &CircuitBreaker[TRequest, TResponse]{
		mx:             &sync.Mutex{},
		timeout:        timeout,
		recoverTimeout: recoverTimeout,
		status:         StatusClosed,
		errorThreshold: errorThreshold,
		halfOpenLimit:  halfOpenLimit,
	}
}

func (cb *CircuitBreaker[TRequest, TResponse]) Execute(ctx context.Context, params TRequest, f func(context.Context, TRequest) (TResponse, error)) (TResponse, error) {
	cb.mx.Lock()

	if cb.status == StatusOpen {
		cb.mx.Unlock()
		return *new(TResponse), ErrCircuitOpened
	}

	cb.totalResponses++
	cb.mx.Unlock()

	ctx, cancel := context.WithTimeout(ctx, cb.timeout)
	defer cancel()

	type response struct {
		result TResponse
		err    error
	}

	ch := make(chan response, 1)

	go func() {
		defer close(ch)

		if ctx.Err() != nil {
			return
		}

		result, err := f(ctx, params)
		ch <- response{result: result, err: err}
	}()

	select {
	case <-ctx.Done():
		cb.handleError()

		return *new(TResponse), ctx.Err()
	case result := <-ch:
		if result.err != nil {
			cb.handleError()

			return *new(TResponse), result.err
		}

		cb.handleSuccessResponse()

		return result.result, nil
	}
}

func (cb *CircuitBreaker[TRequest, TResponse]) incrementErrors() {
	cb.mx.Lock()
	defer cb.mx.Unlock()

	cb.errorResponses++
	cb.totalResponses++
	cb.successResponses = 0
}

func (cb *CircuitBreaker[TRequest, TResponse]) incrementSuccesses() {
	cb.mx.Lock()
	defer cb.mx.Unlock()

	cb.successResponses++
	cb.totalResponses++
}

func (cb *CircuitBreaker[TRequest, TResponse]) setStatus(status Status) {
	cb.mx.Lock()
	defer cb.mx.Unlock()

	cb.status = status
}

func (cb *CircuitBreaker[TRequest, TResponse]) resetCounters() {
	cb.errorResponses = 0
	cb.successResponses = 0
	cb.totalResponses = 0
}

func (cb *CircuitBreaker[TRequest, TResponse]) recover() {
	time.Sleep(cb.recoverTimeout)

	cb.mx.Lock()
	defer cb.mx.Unlock()

	if cb.status == StatusOpen {
		cb.resetCounters()
		cb.setStatus(StatusHalfOpen)

		return
	}
}

func (cb *CircuitBreaker[TRequest, TResponse]) handleError() {
	cb.incrementErrors()

	percentage := (float64(cb.errorResponses) / float64(cb.totalResponses)) * 100

	if percentage >= cb.errorThreshold || (cb.status == StatusHalfOpen && cb.errorResponses >= cb.halfOpenLimit) {
		cb.setStatus(StatusOpen)

		go cb.recover()
	}
}

func (cb *CircuitBreaker[TRequest, TResponse]) handleSuccessResponse() {
	cb.incrementSuccesses()

	if cb.status == StatusHalfOpen && cb.successResponses >= cb.halfOpenLimit {
		cb.setStatus(StatusClosed)
	}
}
