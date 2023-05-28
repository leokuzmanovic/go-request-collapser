package requestcollapser

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type (
	collapserRequest[T any, P comparable] struct {
		param         *P
		resultChannel *chan *collapserResponse[T, P]
	}

	collapserResponse[T any, P comparable] struct {
		param  *P
		result *T
		err    error
	}

	RequestCollapser[T any, P comparable] struct {
		batchCommand              func(context.Context, []*P) (map[P]*T, error)
		fallbackCommand           func(context.Context, *P) (*T, error)
		deepCopyCommand           func(*T) (*T, error)
		intervalInMilis           int64
		maxBatchSize              int
		collapserRequestsChannel  chan *collapserRequest[T, P]
		requestsProcessorNotifier chan *[]*collapserRequest[T, P]
		requestsBatch             *[]*collapserRequest[T, P]
		requestsBatchMutex        *sync.RWMutex
		batchCommandCancelTimeout time.Duration
		diagnosticsEnabled        bool
		shouldStop                atomic.Bool
	}
)

const (
	MAX_BATCH_TIMEOUT = time.Duration(2147483647) * time.Millisecond
	MAX_QUEUE_SIZE    = 2147483647
)

func (m *RequestCollapser[T, P]) withFallbackCommand(fallbackCommand func(ctx context.Context, param *P) (*T, error)) {
	m.fallbackCommand = fallbackCommand
}

func (m *RequestCollapser[T, P]) withDeepCopyCommand(deepCopyCommand func(source *T) (*T, error)) {
	if deepCopyCommand != nil {
		m.deepCopyCommand = deepCopyCommand
	}
}

func (m *RequestCollapser[T, P]) withMaxBatchSize(maxBatchSize int) {
	if maxBatchSize > 0 {
		m.collapserRequestsChannel = make(chan *collapserRequest[T, P], maxBatchSize)
		m.maxBatchSize = maxBatchSize
	}
}

func (m *RequestCollapser[T, P]) withBatchCommandCancelTimeout(batchCommandCancelTimeoutMillis int64) {
	var batchTimeout time.Duration
	if batchCommandCancelTimeoutMillis <= 0 {
		batchTimeout = MAX_BATCH_TIMEOUT
	} else {
		batchTimeout = time.Duration(batchCommandCancelTimeoutMillis) * time.Millisecond
	}
	m.batchCommandCancelTimeout = batchTimeout
}

func (m *RequestCollapser[T, P]) withDiagnosticEnabled(diagnosticsEnabled bool) {
	m.diagnosticsEnabled = diagnosticsEnabled
}

func (m *RequestCollapser[T, P]) Start() {
	// go routine that runs in perpetual loop, accepts requests and adds them to the batch,
	// can also (if configured) trigger the processor if the batch is full
	m.startRequestAcceptor()
	// go routine that runs in perpetual loop and triggers the processor every interval (if there are requests in the batch)
	m.startRequestProcessorTicker()
	// go routine that runs in perpetual loop and processes the batch once notified with the batch
	m.startRequestProcessor()
}

func (m *RequestCollapser[T, P]) Stop() {
	m.shouldStop.Store(true)
	m.collapserRequestsChannel <- nil
	m.requestsProcessorNotifier <- nil
}

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
		maxBatchSize:              MAX_QUEUE_SIZE, // by default, we make it large enough to hold practically all the requests
		collapserRequestsChannel:  make(chan *collapserRequest[T, P], MAX_QUEUE_SIZE),
		requestsProcessorNotifier: make(chan *[]*collapserRequest[T, P], 1),
		requestsBatch:             &initialBatch,
		requestsBatchMutex:        &sync.RWMutex{},
		batchCommandCancelTimeout: MAX_BATCH_TIMEOUT, // by default, we use the MAX_BATCH_TIMEOUT
		diagnosticsEnabled:        false,
		shouldStop:                atomic.Bool{},
	}
	return p, nil
}

func jsonMarshalDeepCopyCommand[T any](source *T) (*T, error) {
	var replicant T
	b, _ := json.Marshal(source)
	err := json.Unmarshal(b, &replicant)
	return &replicant, err
}

// Get sends a request to the collapser and waits for the result /**
func (m *RequestCollapser[T, P]) Get(ctx context.Context, param P) (*T, error) {
	return m.doGet(ctx, param, MAX_BATCH_TIMEOUT)
}

// GetWithTimeout Get sends a request to the collapser and waits for the result - or times out /**
func (m *RequestCollapser[T, P]) GetWithTimeout(ctx context.Context, param P, timeoutInMillis int64) (*T, error) {
	var timeout time.Duration
	if timeoutInMillis <= 0 {
		timeout = MAX_BATCH_TIMEOUT
	} else {
		timeout = time.Duration(timeoutInMillis) * time.Millisecond
	}
	return m.doGet(ctx, param, timeout)
}

func (m *RequestCollapser[T, P]) doGet(ctx context.Context, param P, timeout time.Duration) (*T, error) {
	// send the request
	channel := make(chan *collapserResponse[T, P], 1)
	cr := &collapserRequest[T, P]{
		param:         &param,
		resultChannel: &channel,
	}
	m.collapserRequestsChannel <- cr

	// wait for the response
	var val *collapserResponse[T, P]
	select {
	case val = <-channel:
	case <-ctx.Done():
		fmt.Println("context cancelled while waiting for the response")
	case <-time.After(timeout):
		fmt.Println("timeout waiting for the response")
	}

	// check the result
	if val == nil || val.err != nil {
		if m.fallbackCommand != nil {
			return m.fallbackCommand(ctx, &param)
		}
	}
	if val == nil {
		return nil, fmt.Errorf("no response from the collapser")
	}

	return val.result, val.err
}

/**
 * Adds the request to the batch and checks if processor needs to be notified immediately.
 */
func (m *RequestCollapser[T, P]) startRequestAcceptor() {
	go func() {
		for request := range m.collapserRequestsChannel { // blocks until there is a request in the channel
			if request == nil || m.shouldStop.Load() {
				break
			}
			requestsBatchSize := m.appendRequestToBatch(request)
			fmt.Println("added request to batch - requestsBatchSize", requestsBatchSize)

			// check if the batch is full and needs to be sent to the processor immediately
			if m.maxBatchSize > 0 && requestsBatchSize >= m.maxBatchSize {
				batch := m.prepareBatchForProcessing()
				if batch != nil && len(*batch) > 0 { // double check that the batch is not empty (because of concurrency)
					m.requestsProcessorNotifier <- batch
				}
			}
		}
	}()
}

func (m *RequestCollapser[T, P]) startRequestProcessorTicker() {
	go func() {
		time.Sleep(time.Duration(m.intervalInMilis) * time.Millisecond)
		for {
			if m.shouldStop.Load() {
				break
			}
			start := time.Now().UnixNano() / int64(time.Millisecond)
			batch := m.prepareBatchForProcessing()
			if batch == nil || len(*batch) <= 0 { // double check that the batch is not empty (because of concurrency)
				sleepIfNeedBe(start, m.intervalInMilis)
				continue
			}
			// send the batch to the processor
			m.requestsProcessorNotifier <- batch
			sleepIfNeedBe(start, m.intervalInMilis)
		}
	}()
}

func (m *RequestCollapser[T, P]) startRequestProcessor() {
	go func() {
		for batch := range m.requestsProcessorNotifier { // blocks until there is a batch to process (publishers are: )
			if batch == nil || m.shouldStop.Load() {
				break
			}

			params := getParameters(batch)

			// prepare the cancellable context for the batch command
			var cancelTimeout = m.batchCommandCancelTimeout
			ctx, cancel := context.WithCancel(context.Background())
			time.AfterFunc(cancelTimeout, cancel)

			results, err := m.batchCommand(ctx, params)
			if ctx.Err() != nil {
				fmt.Println("context error when performing batch command", ctx.Err())
				err = ctx.Err()
			}
			m.distributeResults(batch, results, err)
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

func (m *RequestCollapser[T, P]) distributeResults(batch *[]*collapserRequest[T, P], results map[P]*T, err error) {
	returningError := err
	if returningError != nil {
		fmt.Println("error while executing batch command", err)
	}

	resultPointersControlMap := make(map[string]bool)
	for _, request := range *batch { // for each request in the batch try to find result and send it to the request channel
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
				*request.resultChannel <- &collapserResponse[T, P]{
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
		*request.resultChannel <- &collapserResponse[T, P]{
			param:  request.param,
			result: nil,
			err:    returningError,
		}
		// it is a good practice for a sender to close the channel
		close(*request.resultChannel)
	}
}

func getParameters[T any, P comparable](requests *[]*collapserRequest[T, P]) []*P {
	// use map to remove duplicates
	paramMap := make(map[P]bool)
	for _, request := range *requests {
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

/**
 * Returns the current batch and resets the batch to an empty batch.
 */
func (m *RequestCollapser[T, P]) prepareBatchForProcessing() *[]*collapserRequest[T, P] {
	defer m.requestsBatchMutex.Unlock()
	m.requestsBatchMutex.Lock()

	var batch []*collapserRequest[T, P]
	batch = *m.requestsBatch
	if batch == nil || len(batch) <= 0 {
		return nil
	}
	emptyBatch := make([]*collapserRequest[T, P], 0)
	m.requestsBatch = &emptyBatch
	return &batch
}

func (m *RequestCollapser[T, P]) appendRequestToBatch(request *collapserRequest[T, P]) int {
	defer m.requestsBatchMutex.Unlock()
	m.requestsBatchMutex.Lock()

	*m.requestsBatch = append(*m.requestsBatch, request)
	requestsBatchSize := len(*m.requestsBatch)
	return requestsBatchSize
}
