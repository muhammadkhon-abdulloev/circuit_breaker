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
	// timeout - лимит обработки запроса
	timeout time.Duration
	// recoverTimeout - сколько секунд предохранитель будет в статусе opened
	recoverTimeout time.Duration
	// status - текущий статус предохранителя
	status Status
	// errorThreshold - при каком количестве зафейленных запросов переключаться на статус opened
	errorThreshold float64
	// halfOpenLimit - лимит запросов в статусе halfOpen после чего статус предохранителя перейдет на другой
	halfOpenLimit int64
	// responsesThreshold - количество последних запросов которые будут учитываться при подсчете errorThreshold
	responsesThreshold int64
	// responses - хранит в себе результаты запросов (fail - false, success - true)
	responses []bool
}

func NewCB[TRequest, TResponse any](timeout, recoverTimeout time.Duration, errorThreshold float64, halfOpenLimit int64, responsesThreshold int64) *CircuitBreaker[TRequest, TResponse] {
	return &CircuitBreaker[TRequest, TResponse]{
		mx:                 &sync.Mutex{},
		timeout:            timeout,
		recoverTimeout:     recoverTimeout,
		status:             StatusClosed,
		errorThreshold:     errorThreshold,
		halfOpenLimit:      halfOpenLimit,
		responsesThreshold: responsesThreshold,
		responses:          make([]bool, 0, responsesThreshold+1),
	}
}

func (cb *CircuitBreaker[TRequest, TResponse]) Execute(ctx context.Context, params TRequest, f func(context.Context, TRequest) (TResponse, error)) (TResponse, error) {
	cb.mx.Lock()
	if cb.status == StatusOpen {
		cb.mx.Unlock()
		return *new(TResponse), ErrCircuitOpened
	}
	cb.mx.Unlock()

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
		cb.handleResponse(false)

		return *new(TResponse), ctx.Err()
	case result := <-ch:
		if result.err != nil {
			cb.handleResponse(false)

			return *new(TResponse), result.err
		}

		cb.handleResponse(true)

		return result.result, nil
	}
}

func (cb *CircuitBreaker[TRequest, TResponse]) handleResponse(response bool) {
	cb.addResponse(response)

	var (
		errorsCount    int64 = 0
		successesCount int64 = 0
	)

	for _, res := range cb.responses {
		if res {
			successesCount++
		} else {
			errorsCount++
			successesCount = 0
		}
	}

	cb.handleStatus(successesCount, errorsCount)
}

func (cb *CircuitBreaker[TRequest, TResponse]) addResponse(response bool) {
	cb.mx.Lock()
	defer cb.mx.Unlock()

	threshold := int(cb.responsesThreshold)
	if len(cb.responses) >= threshold {
		cb.responses = cb.responses[len(cb.responses)-threshold+1:]
	}

	cb.responses = append(cb.responses, response)
}

func (cb *CircuitBreaker[TRequest, TResponse]) handleStatus(successesCount, errorsCount int64) {
	if cb.status == StatusHalfOpen && successesCount >= cb.halfOpenLimit {
		cb.setStatus(StatusClosed)
	}

	errorsPercentage := float64(errorsCount) / float64(len(cb.responses)) * 100
	if errorsPercentage >= cb.errorThreshold || (cb.status == StatusHalfOpen && errorsCount >= cb.halfOpenLimit) {
		cb.setStatus(StatusOpen)

		go cb.recover()
	}
}

func (cb *CircuitBreaker[TRequest, TResponse]) setStatus(status Status) {
	cb.mx.Lock()
	defer cb.mx.Unlock()

	cb.status = status
	cb.responses = make([]bool, 0, cb.responsesThreshold+1)
}

func (cb *CircuitBreaker[TRequest, TResponse]) recover() {
	time.Sleep(cb.recoverTimeout)

	cb.mx.Lock()

	if cb.status == StatusOpen {
		cb.mx.Unlock()

		cb.setStatus(StatusHalfOpen)

		return
	}

	cb.mx.Unlock()

}
