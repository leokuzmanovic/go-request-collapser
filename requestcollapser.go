package requestcollapser

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type (
	collapserRequest[T any, P comparable] struct {
		param         *P
		resultChannel chan *collapserResponse[T, P]
	}

	collapserResponse[T any, P comparable] struct {
		param  *P
		result *T
		err    error
	}

	NO_RESULT struct{}

	// RequestCollapser allows to collapse multiple requests into one batch request.
	RequestCollapser[T any, P comparable] struct {
		// Command to be executed in batch
		batchCommand func(context.Context, []*P) (map[P]*T, error)
		// Command to be executed in case of batch command failure
		fallbackCommand func(context.Context, *P) (*T, error)
		// Command to be executed to deep copy the result of batch command
		deepCopyCommand func(*T) (*T, error)
		// Interval in milliseconds to trigger the batch command
		intervalInMilis int64
		// Maximum number of requests to be batched together before triggering the batch command
		maxBatchSize int
		// Channel to accept requests
		collapserRequestsChannel chan *collapserRequest[T, P]
		// Channel to notify the processor that there are requests in the batch to be processed
		requestsProcessorNotifier chan []*collapserRequest[T, P]
		// Slice of requests to be batched together
		requestsQueue []*collapserRequest[T, P]
		// Mutex to protect the batch of requests
		requestsBatchMutex *sync.RWMutex
		// Max time to wait for the batch command to complete
		batchCommandCancelTimeout time.Duration
		// Flag to enable/disable diagnostics
		diagnosticsEnabled bool
		// Flag to stop the collapser
		shouldStop            atomic.Bool
		allowDuplicatedParams bool
		isRunning             bool
	}
)

const (
	// Max timeout for batch command
	MAX_BATCH_TIMEOUT = time.Duration(2147483647) * time.Millisecond
	// Max queue size for requests to be batched
	MAX_QUEUE_SIZE = 10000
)

// Provides the fallback command to be executed in case of batch command failure
func (m *RequestCollapser[T, P]) WithFallbackCommand(fallbackCommand func(ctx context.Context, param *P) (*T, error)) error {
	if m.isRunning {
		return errors.New("collapser is already running")
	}
	m.fallbackCommand = fallbackCommand
	return nil
}

// Provides the command to be executed to deep copy the result of batch command
func (m *RequestCollapser[T, P]) WithDeepCopyCommand(deepCopyCommand func(source *T) (*T, error)) error {
	if m.isRunning {
		return errors.New("collapser is already running")
	}
	if deepCopyCommand != nil {
		m.deepCopyCommand = deepCopyCommand
	}
	return nil
}

// Provides the limit of requests to be batched together before triggering the batch command
func (m *RequestCollapser[T, P]) WithMaxBatchSize(maxBatchSize int) error {
	if m.isRunning {
		return errors.New("collapser is already running")
	}
	if maxBatchSize > 0 {
		m.maxBatchSize = maxBatchSize
	}
	return nil
}

// Provides the max time to wait for the batch command to complete
func (m *RequestCollapser[T, P]) WithBatchCommandTimeout(batchCommandCancelTimeoutMillis int64) error {
	if m.isRunning {
		return errors.New("collapser is already running")
	}
	var batchTimeout time.Duration
	if batchCommandCancelTimeoutMillis <= 0 {
		batchTimeout = MAX_BATCH_TIMEOUT
	} else {
		batchTimeout = time.Duration(batchCommandCancelTimeoutMillis) * time.Millisecond
	}
	m.batchCommandCancelTimeout = batchTimeout
	return nil
}

func (m *RequestCollapser[T, P]) WithAllowDuplicatedParams(allowDuplicatedParams bool) error {
	if m.isRunning {
		return errors.New("collapser is already running")
	}
	m.allowDuplicatedParams = allowDuplicatedParams
	return nil
}

// Provides the diagnostics flag. If set, logs will be printed to stdout
func (m *RequestCollapser[T, P]) WithDiagnosticEnabled(diagnosticsEnabled bool) error {
	if m.isRunning {
		return errors.New("collapser is already running")
	}
	m.diagnosticsEnabled = diagnosticsEnabled
	return nil
}

// Creates a new RequestCollapser instance
func NewRequestCollapser[T any, P comparable](
	batchCommand func(ctx context.Context, params []*P) (map[P]*T, error),
	batchCommandIntervalMillis int64,
) (*RequestCollapser[T, P], error) {
	if batchCommand == nil {
		return nil, fmt.Errorf("batchCommand cannot be nil")
	}
	if batchCommandIntervalMillis <= 0 {
		return nil, fmt.Errorf("batchCommandIntervalMillis must be greater than 0")
	}
	initialBatch := make([]*collapserRequest[T, P], 0)

	p := &RequestCollapser[T, P]{
		batchCommand:              batchCommand,
		deepCopyCommand:           jsonMarshalDeepCopyCommand[T],
		intervalInMilis:           batchCommandIntervalMillis,
		maxBatchSize:              MAX_QUEUE_SIZE,
		collapserRequestsChannel:  make(chan *collapserRequest[T, P], MAX_QUEUE_SIZE),   // to avoid blocking: MAX_QUEUE_SIZE
		requestsProcessorNotifier: make(chan []*collapserRequest[T, P], MAX_QUEUE_SIZE), // to avoid blocking: MAX_QUEUE_SIZE
		requestsQueue:             initialBatch,
		requestsBatchMutex:        &sync.RWMutex{},
		batchCommandCancelTimeout: MAX_BATCH_TIMEOUT, // by default, we use the MAX_BATCH_TIMEOUT
		diagnosticsEnabled:        false,
		shouldStop:                atomic.Bool{},
		allowDuplicatedParams:     false,
	}
	return p, nil
}

// Starts the collapser: starts the request acceptor, request processor ticker and request processor
func (m *RequestCollapser[T, P]) Start() {
	m.isRunning = true
	// go routine that runs in perpetual loop, accepts requests and adds them to the batch,
	// can also (if configured) trigger the processor if the batch is full
	m.runRequestAcceptor()
	// go routine that runs in perpetual loop and triggers the processor every interval (if there are requests in the batch)
	m.runRequestProcessorTicker()
	// go routine that runs in perpetual loop and processes the batch once notified with the batch
	m.runRequestProcessor()
	m.log("RequestCollapser::Start - collapser started")
}

// Stops the collapser: stops the request acceptor, request processor ticker and request processor
func (m *RequestCollapser[T, P]) Stop() {
	m.isRunning = false
	m.shouldStop.Store(true)
	m.collapserRequestsChannel <- nil
	m.requestsProcessorNotifier <- nil
	m.log("RequestCollapser::Stop - collapser stopped")
}

func jsonMarshalDeepCopyCommand[T any](source *T) (*T, error) {
	var replicant T
	b, _ := json.Marshal(source)
	err := json.Unmarshal(b, &replicant)
	return &replicant, err
}

// Get sends a request to the collapser and waits for the result
func (m *RequestCollapser[T, P]) Get(ctx context.Context, param P) (*T, error) {
	return m.doGet(ctx, param, MAX_BATCH_TIMEOUT)
}

// GetWithTimeout Get sends a request to the collapser and waits for the result - or times out
func (m *RequestCollapser[T, P]) GetWithTimeout(ctx context.Context, param P, timeoutInMillis int64) (*T, error) {
	if timeoutInMillis <= 0 {
		return nil, errors.New("timeoutInMillis must be greater than 0")
	}
	var timeout = time.Duration(timeoutInMillis) * time.Millisecond
	return m.doGet(ctx, param, timeout)
}

func (m *RequestCollapser[T, P]) QueueRequest(param P) error {
	err := m.checkIsStopped()
	if err != nil {
		return err
	}

	cr := &collapserRequest[T, P]{
		param: &param,
	}
	return m.tryQueueRequest(param, cr)
}

func (m *RequestCollapser[T, P]) doGet(ctx context.Context, param P, timeout time.Duration) (*T, error) {
	err := m.checkIsStopped()
	if err != nil {
		return nil, err
	}

	// try to send the request
	channel := make(chan *collapserResponse[T, P], 1)
	cr := &collapserRequest[T, P]{
		param:         &param,
		resultChannel: channel,
	}
	err = m.tryQueueRequest(param, cr) // do not wait for the queue to be available, if the default queue size is not enough just try fallback
	if err != nil {
		m.log("RequestCollapser::doGet - could not queue request to the collapser")
		if m.fallbackCommand != nil {
			m.log("RequestCollapser::doGet - invoking fallback command")
			return m.fallbackCommand(ctx, &param)
		}
		return nil, err
	}

	// wait for the response
	var val *collapserResponse[T, P]
	select {
	case val = <-channel:
	case <-ctx.Done():
		m.log("RequestCollapser::doGet - context cancelled while waiting for the response")
	case <-time.After(timeout):
		m.log("RequestCollapser::doGet - timeout waiting for the response")
	}

	// check the result that should always be initialised even if the batch response is nil or there was an error
	if val == nil || val.err != nil {
		m.log("RequestCollapser::doGet - could not get value from the collapser")
		if m.fallbackCommand != nil {
			m.log("RequestCollapser::doGet - invoking fallback command")
			return m.fallbackCommand(ctx, &param)
		}
	}
	if val == nil {
		errorMessage := "no response from the collapser"
		m.log(fmt.Sprintf("RequestCollapser::doGet - %s", errorMessage))
		return nil, fmt.Errorf(errorMessage)
	}

	return val.result, val.err
}

func (m *RequestCollapser[T, P]) tryQueueRequest(param P, cr *collapserRequest[T, P]) error {
	var sendErr error
	select {
	case m.collapserRequestsChannel <- cr:
		m.log("RequestCollapser::QueueRequest - request queued")
	default:
		errorMessage := fmt.Sprintf("RequestCollapser::QueueRequest - collapser queue is full, dropping request: %v", param)
		m.log(errorMessage)
		sendErr = errors.New(errorMessage)
	}
	return sendErr
}

func (m *RequestCollapser[T, P]) checkIsStopped() error {
	if m.shouldStop.Load() {
		errorMessage := "Collapser is stopped!"
		m.log(fmt.Sprintf("RequestCollapser::checkIsStopped - %s", errorMessage))
		return fmt.Errorf(errorMessage)
	}
	return nil
}

/**
 * Adds the request to the batch and checks if processor needs to be notified immediately.
 */
func (m *RequestCollapser[T, P]) runRequestAcceptor() {
	go func() {
		for request := range m.collapserRequestsChannel { // blocks until there is a request in the channel
			if request == nil || m.shouldStop.Load() {
				m.log("RequestCollapser::runRequestAcceptor - stopping the acceptor")
				break
			}
			requestsBatchSize := m.appendRequestToBatch(request)

			// check if the batch is full and needs to be sent to the processor immediately
			if m.maxBatchSize > 0 && requestsBatchSize >= m.maxBatchSize {
				batch := m.prepareBatchForProcessing()
				if batch != nil && len(batch) > 0 { // double check that the batch is not empty (because of concurrency)
					m.requestsProcessorNotifier <- batch
				}
			}
		}
	}()
}

func (m *RequestCollapser[T, P]) runRequestProcessorTicker() {
	go func() {
		time.Sleep(time.Duration(m.intervalInMilis) * time.Millisecond)
		for {
			if m.shouldStop.Load() {
				m.log("RequestCollapser::runRequestProcessorTicker - stopping the ticker")
				break
			}
			start := time.Now().UnixNano() / int64(time.Millisecond)
			batch := m.prepareBatchForProcessing()
			if batch == nil || len(batch) <= 0 { // double check that the batch is not empty (because of concurrency)
				sleepIfNeedBe(start, m.intervalInMilis)
				continue
			}
			// send the batch to the processor
			m.requestsProcessorNotifier <- batch
			sleepIfNeedBe(start, m.intervalInMilis)
		}
	}()
}

func (m *RequestCollapser[T, P]) runRequestProcessor() {
	go func() { // listening for batches to process
		for batch := range m.requestsProcessorNotifier { // blocks until there is a batch to process
			if batch == nil || m.shouldStop.Load() {
				m.log("RequestCollapser::runRequestProcessor - stopping the processor")
				break
			}
			go func(batch []*collapserRequest[T, P]) { // process the batch in a separate goroutine
				params := m.getParameters(batch)

				// prepare the cancellable context for the batch command
				var cancelTimeout = m.batchCommandCancelTimeout
				ctx, cancel := context.WithCancel(context.Background())
				time.AfterFunc(cancelTimeout, cancel)

				results, err := m.batchCommand(ctx, params)
				if ctx.Err() != nil {
					m.log(fmt.Sprintf("RequestCollapser::runRequestProcessor - context error when performing batch command: %s ", ctx.Err()))
					err = ctx.Err()
				}
				m.distributeResults(batch, results, err)
			}(batch)
		}
	}()
}

func sleepIfNeedBe(start int64, intervalInMillis int64) {
	end := time.Now().UnixNano() / int64(time.Millisecond)
	processingTime := end - start // in ms
	if processingTime < intervalInMillis {
		// sleep for the remaining time
		time.Sleep(time.Duration(intervalInMillis-processingTime) * time.Millisecond)
	}
}

func (m *RequestCollapser[T, P]) distributeResults(batch []*collapserRequest[T, P], results map[P]*T, err error) {
	returningError := err
	if returningError != nil {
		m.log(fmt.Sprintf("RequestCollapser::distributeResults - error while executing batch command: %s ", err))
	}

	resultPointersControlMap := make(map[string]bool)
	for _, request := range batch { // for each request in the batch try to find result and send it to the request channel
		if request.resultChannel == nil {
			m.log("RequestCollapser::distributeResults - result channel is nil, skipping the request")
			continue
		}
		if results != nil && err == nil {
			var result = results[*request.param]
			// check if we need to make a copy of the result (is expensive in the default mode) and send it to the request channel
			var newResult *T
			var copyError error = nil
			resultPointerAddress := fmt.Sprintf("%p", result)
			_, ok := resultPointersControlMap[resultPointerAddress]
			if ok {
				newResult, copyError = m.deepCopyCommand(result)
				resultPointerAddress = fmt.Sprintf("%p", newResult)
			} else {
				newResult = result
			}
			resultPointersControlMap[resultPointerAddress] = true

			if copyError == nil {
				request.resultChannel <- &collapserResponse[T, P]{
					param:  request.param,
					result: newResult,
					err:    returningError,
				}
				continue
			} else {
				returningError = copyError
			}
		}
		// could not get the proper response
		request.resultChannel <- &collapserResponse[T, P]{
			param:  request.param,
			result: nil,
			err:    returningError,
		}
		// it is a good practice for the sender to close the channel
		close(request.resultChannel)
	}
}

func (m *RequestCollapser[T, P]) getParameters(requests []*collapserRequest[T, P]) []*P {
	if m.allowDuplicatedParams {
		return parseParamsFromRequests(requests)
	}

	// use map to remove duplicates
	paramMap := make(map[P]bool)
	for _, request := range requests {
		if request.param == nil {
			continue
		}
		paramMap[*request.param] = true
	}
	if len(paramMap) == 0 {
		return nil
	}

	var params = make([]*P, 0, len(paramMap))
	for param := range paramMap {
		var paramFromMap P = param
		params = append(params, &paramFromMap)
	}

	return params
}

func parseParamsFromRequests[T any, P comparable](requests []*collapserRequest[T, P]) []*P {
	var params = make([]*P, 0, len(requests))
	for _, request := range requests {
		if request.param == nil {
			continue
		}
		params = append(params, request.param)
	}
	return params
}

/**
 * Returns the current batch and resets the batch to an empty batch.
 */
func (m *RequestCollapser[T, P]) prepareBatchForProcessing() []*collapserRequest[T, P] {
	defer m.requestsBatchMutex.Unlock()
	m.requestsBatchMutex.Lock()

	if m.requestsQueue == nil {
		return nil
	}
	currentQueueSize := len(m.requestsQueue)
	if currentQueueSize <= 0 {
		return nil
	}

	var batch []*collapserRequest[T, P]
	if currentQueueSize > m.maxBatchSize {
		// NOTE: even though case is not possible right now, it could be if the maxBatchSize check is removed from appendRequestToBatch method
		// take only the max batch size requests to the batch and remove them from the queue
		batch = (m.requestsQueue)[:m.maxBatchSize]
		m.requestsQueue = (m.requestsQueue)[m.maxBatchSize:]
	} else {
		// put all requests in the batch and reset the queue
		batch = (m.requestsQueue)[:currentQueueSize]
		emptyBatch := make([]*collapserRequest[T, P], 0)
		m.requestsQueue = emptyBatch
	}
	return batch
}

func (m *RequestCollapser[T, P]) appendRequestToBatch(request *collapserRequest[T, P]) int {
	defer m.requestsBatchMutex.Unlock()
	m.requestsBatchMutex.Lock()

	m.requestsQueue = append(m.requestsQueue, request)
	requestsBatchSize := len(m.requestsQueue)
	return requestsBatchSize
}

func (m *RequestCollapser[T, P]) log(message ...string) {
	if m.diagnosticsEnabled {
		fmt.Println(message)
	}
}
