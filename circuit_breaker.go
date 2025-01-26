package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
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
	// errorResponsesCount - количество зафейленных запросов
	errorResponsesCount atomic.Int64
	// successResponsesCount - количество успешных запросов
	successResponsesCount atomic.Int64
	// totalExecutionsCount - общее количество запросов
	totalExecutionsCount atomic.Int64
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
	cb.mx.Unlock()

	cb.totalExecutionsCount.Add(1)

	ctx, cancel := context.WithTimeout(ctx, cb.timeout)
	defer cancel()

	type response struct {
		result TResponse
		err    error
	}

	ch := make(chan response)

	go func() {
		defer close(ch)

		result, err := f(ctx, params)

		select {
		case <-ctx.Done():
		case ch <- response{result: result, err: err}:
		}

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

	cb.errorResponsesCount.Add(1)
	cb.totalExecutionsCount.Add(1)
	cb.successResponsesCount.Store(0)
}

func (cb *CircuitBreaker[TRequest, TResponse]) incrementSuccesses() {
	cb.successResponsesCount.Add(1)
	cb.totalExecutionsCount.Add(1)
}

func (cb *CircuitBreaker[TRequest, TResponse]) setStatus(status Status) {
	cb.mx.Lock()
	defer cb.mx.Unlock()

	cb.status = status
}

func (cb *CircuitBreaker[TRequest, TResponse]) resetCounters() {
	cb.errorResponsesCount.Store(0)
	cb.successResponsesCount.Store(0)
	cb.totalExecutionsCount.Store(0)
}

func (cb *CircuitBreaker[TRequest, TResponse]) recover() {
	time.Sleep(cb.recoverTimeout)

	cb.mx.Lock()

	if cb.status == StatusOpen {
		cb.mx.Unlock()

		cb.resetCounters()
		cb.setStatus(StatusHalfOpen)

		return
	}

	cb.mx.Unlock()
}

func (cb *CircuitBreaker[TRequest, TResponse]) handleError() {
	cb.incrementErrors()

	errorsPercentage := float64(cb.errorResponsesCount.Load()) / float64(cb.totalExecutionsCount.Load()) * 100

	if errorsPercentage >= cb.errorThreshold || (cb.status == StatusHalfOpen && cb.errorResponsesCount.Load() >= cb.halfOpenLimit) {
		cb.setStatus(StatusOpen)

		go cb.recover()
	}
}

func (cb *CircuitBreaker[TRequest, TResponse]) handleSuccessResponse() {
	cb.incrementSuccesses()

	if cb.status == StatusHalfOpen && cb.successResponsesCount.Load() >= cb.halfOpenLimit {
		cb.setStatus(StatusClosed)
	}
}
