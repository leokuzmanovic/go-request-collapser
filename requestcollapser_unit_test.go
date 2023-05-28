package requestcollapser

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gitlab.com/knowunity/go-common/pkg/errors"
)

type TestResult struct {
	ID     *string
	Source string
}
type TestResultWithParam struct {
	Result *TestResult
	Param  string
	Error  error
}

var (
	batchCommandSuccessful = func(ctx context.Context, params []*string) (map[string]*TestResult, error) {
		results := make(map[string]*TestResult)
		for _, param := range params {
			id := *param
			results[*param] = &TestResult{ID: &id, Source: "batch"}
		}
		return results, nil
	}

	fallbackCommandSuccessful = func(ctx context.Context, param *string) (*TestResult, error) {
		id := *param
		return &TestResult{ID: &id, Source: "fallback"}, nil
	}

	copyCommandSuccessful = func(original *TestResult) (*TestResult, error) {
		newTestResult := TestResult{}
		id := *original.ID
		newTestResult.ID = &id
		newTestResult.Source = original.Source
		return &newTestResult, nil
	}
)

func TestUnit_RequestCollapser(t *testing.T) {
	t.Run("invalid batch command", func(t *testing.T) {
		rc, err := NewRequestCollapser[TestResult, string](nil, 100)
		assert.Nil(t, rc)
		assert.Error(t, err)
		fmt.Println("END 1")
	})

	t.Run("invalid batch command interval", func(t *testing.T) {
		rc, err := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 0)
		assert.Nil(t, rc)
		assert.Error(t, err)
		fmt.Println("END 2")
	})

	t.Run("get one result", func(t *testing.T) {
		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 20)
		rc.withFallbackCommand(fallbackCommandSuccessful)
		rc.withMaxBatchSize(20)
		rc.withBatchCommandCancelTimeout(0)
		rc.Start()

		var result *TestResult
		var err error

		wg := sync.WaitGroup{}
		wg.Add(1)
		param := "param1"
		go func() {
			result, err = rc.Get(context.Background(), param)
			wg.Done()
		}()
		wg.Wait()

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, *result.ID, "param1")
		fmt.Println("END 3")
	})

	t.Run("get multiple results", func(t *testing.T) {
		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 20)
		rc.withFallbackCommand(fallbackCommandSuccessful)
		rc.withMaxBatchSize(20)
		rc.withBatchCommandCancelTimeout(0)
		rc.Start()

		params := []string{"test0", "test1", "test2", "test3", "test4", "test5", "test6", "test7", "test8", "test9"}
		results := getTestResults(params, rc)

		assertBatchSucceeded(t, results, len(params), "batch")
		fmt.Println("END 4")
	})

	t.Run("duplicated params", func(t *testing.T) {
		batchParamArgs := make([]*string, 0)
		batchFunction := func(ctx context.Context, params []*string) (map[string]*TestResult, error) {
			batchParamArgs = params
			results := make(map[string]*TestResult)
			index := 0
			for _, param := range params {
				id := *param
				results[*param] = &TestResult{ID: &id, Source: "batch"}
				index++
			}
			return results, nil
		}

		rc, _ := NewRequestCollapser[TestResult, string](batchFunction, 20)
		rc.withFallbackCommand(fallbackCommandSuccessful)
		rc.withMaxBatchSize(20)
		rc.withBatchCommandCancelTimeout(0)
		rc.Start()

		params := []string{"test0", "test1", "test2", "test2", "test3", "test3", "test3", "test4", "test4", "test4", "test4"}
		results := getTestResults(params, rc)

		assertBatchSucceeded(t, results, len(params), "batch")
		assert.Equal(t, len(batchParamArgs), 5)
		fmt.Println("END 5")
	})

	t.Run("partially missing results", func(t *testing.T) {
		batchFunction := func(ctx context.Context, params []*string) (map[string]*TestResult, error) {
			results := make(map[string]*TestResult)
			index := -1
			for _, param := range params {
				index++
				if *param == "test0" || *param == "test1" || *param == "test2" {
					continue
				}
				id := *param
				results[*param] = &TestResult{ID: &id, Source: "batch"}
			}
			return results, nil
		}

		rc, _ := NewRequestCollapser[TestResult, string](batchFunction, 20)
		rc.withFallbackCommand(fallbackCommandSuccessful)
		rc.withMaxBatchSize(20)
		rc.withBatchCommandCancelTimeout(0)
		rc.Start()

		params := []string{"test0", "test1", "test2", "test3", "test4", "test5", "test6", "test7", "test8", "test9"}
		results := getTestResults(params, rc)

		for _, resultWithParam := range results {
			if resultWithParam.Param == "test0" || resultWithParam.Param == "test1" || resultWithParam.Param == "test2" {
				assert.Nil(t, resultWithParam.Result.ID)
				assert.NoError(t, resultWithParam.Error)
			} else {
				assert.Equal(t, resultWithParam.Result.Source, "batch")
				assert.Equal(t, *resultWithParam.Result.ID, resultWithParam.Param)
				assert.NoError(t, resultWithParam.Error)
			}
		}
		assert.Equal(t, len(results), len(params))
		fmt.Println("END 6")
	})

	t.Run("batch returns empty results", func(t *testing.T) {
		batchFunction := func(ctx context.Context, params []*string) (map[string]*TestResult, error) {
			return make(map[string]*TestResult), nil
		}

		rc, _ := NewRequestCollapser[TestResult, string](batchFunction, 20)
		rc.withFallbackCommand(fallbackCommandSuccessful)
		rc.withMaxBatchSize(20)
		rc.withBatchCommandCancelTimeout(0)
		rc.Start()

		params := []string{"test0", "test1", "test2", "test3", "test4", "test5", "test6", "test7", "test8", "test9"}
		results := getTestResults(params, rc)

		assertEmptyBatch(t, results, params)
		fmt.Println("END 7")
	})

	t.Run("batch return nil results", func(t *testing.T) {
		batchFunction := func(ctx context.Context, params []*string) (map[string]*TestResult, error) {
			return nil, nil
		}

		rc, _ := NewRequestCollapser[TestResult, string](batchFunction, 20)
		rc.withFallbackCommand(fallbackCommandSuccessful)
		rc.withMaxBatchSize(20)
		rc.withBatchCommandCancelTimeout(0)
		rc.Start()

		params := []string{"test0", "test1", "test2", "test3", "test4", "test5", "test6", "test7", "test8", "test9"}
		results := getTestResults(params, rc)

		assertEmptyBatch(t, results, params)
		fmt.Println("END 8")
	})

	t.Run("batch returns error and no fallback provided", func(t *testing.T) {
		batchFunction := func(ctx context.Context, params []*string) (map[string]*TestResult, error) {
			return nil, errors.New("batch error")
		}

		rc, _ := NewRequestCollapser[TestResult, string](batchFunction, 20)
		rc.withMaxBatchSize(20)
		rc.withBatchCommandCancelTimeout(0)
		rc.Start()

		params := []string{"test0", "test1", "test2", "test3", "test4", "test5", "test6", "test7", "test8", "test9"}
		results := getTestResults(params, rc)

		assertBatchWithErrors(t, results, params)
		fmt.Println("END 9")
	})

	t.Run("batch returns error and so does fallback", func(t *testing.T) {
		batchFunction := func(ctx context.Context, params []*string) (map[string]*TestResult, error) {
			return nil, errors.New("batch error")
		}
		fallbackFunction := func(ctx context.Context, params *string) (*TestResult, error) {
			return nil, errors.New("batch error")
		}

		rc, _ := NewRequestCollapser[TestResult, string](batchFunction, 20)
		rc.withFallbackCommand(fallbackFunction)
		rc.withMaxBatchSize(20)
		rc.withBatchCommandCancelTimeout(0)
		rc.Start()

		params := []string{"test0", "test1", "test2"}
		results := getTestResults(params, rc)

		assertBatchWithErrors(t, results, params)
		fmt.Println("END 10")
	})

	t.Run("batch returns error but fallback returns results", func(t *testing.T) {
		batchFunction := func(ctx context.Context, params []*string) (map[string]*TestResult, error) {
			return nil, errors.New("batch error")
		}
		fallbackFunction := func(ctx context.Context, param *string) (*TestResult, error) {
			id := *param
			return &TestResult{ID: &id, Source: "fallback"}, nil
		}

		rc, _ := NewRequestCollapser[TestResult, string](batchFunction, 20)
		rc.withFallbackCommand(fallbackFunction)
		rc.withMaxBatchSize(20)
		rc.withBatchCommandCancelTimeout(0)
		rc.Start()

		params := []string{"test0", "test1", "test2"}
		results := getTestResults(params, rc)

		assertBatchSucceeded(t, results, len(params), "fallback")
		fmt.Println("END 11")
	})

	t.Run("batch returns error but fallback returns partial results", func(t *testing.T) {
		batchFunction := func(ctx context.Context, params []*string) (map[string]*TestResult, error) {
			return nil, errors.New("batch error")
		}
		fallbackFunction := func(ctx context.Context, param *string) (*TestResult, error) {
			if *param == "test0" || *param == "test2" {
				return &TestResult{Source: "fallback"}, errors.New("fallback error")
			}
			id := *param
			return &TestResult{ID: &id, Source: "fallback"}, nil
		}

		rc, _ := NewRequestCollapser[TestResult, string](batchFunction, 20)
		rc.withFallbackCommand(fallbackFunction)
		rc.withMaxBatchSize(20)
		rc.withBatchCommandCancelTimeout(0)
		rc.Start()

		params := []string{"test0", "test1", "test2", "test3"}
		results := getTestResults(params, rc)

		for _, resultWithParam := range results {
			if resultWithParam.Param == "test0" || resultWithParam.Param == "test2" {
				assert.Nil(t, resultWithParam.Result.ID)
				assert.Error(t, resultWithParam.Error)
			} else {
				assert.Equal(t, *resultWithParam.Result.ID, resultWithParam.Param)
				assert.NoError(t, resultWithParam.Error)
			}
			assert.Equal(t, resultWithParam.Result.Source, "fallback")
		}
		assert.Equal(t, len(results), 4)
		fmt.Println("END 12")
	})

	t.Run("collapser ticker ticks before all requests are aggregated", func(t *testing.T) {
		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 10)
		rc.withFallbackCommand(fallbackCommandSuccessful)
		rc.withMaxBatchSize(20)
		rc.withBatchCommandCancelTimeout(0)
		rc.Start()

		params := []string{"test0", "test1", "test2", "test3"}
		results := getTestResultsWithTimeoutBetweenRequests(params, rc, 10*time.Millisecond)

		assertBatchSucceeded(t, results, len(params), "batch")
		fmt.Println("END 13")
	})

	t.Run("collapser buffer gets full before the ticker", func(t *testing.T) {
		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 100)
		rc.withFallbackCommand(fallbackCommandSuccessful)
		rc.withMaxBatchSize(4)
		rc.withBatchCommandCancelTimeout(0)
		rc.Start()

		params := []string{"test0", "test1", "test2", "test3", "test4"}
		results := getTestResults(params, rc)

		assertBatchSucceeded(t, results, len(params), "batch")
		fmt.Println("END 14")
	})

	t.Run("batch command is faster than the batch timeout", func(t *testing.T) {
		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 20)
		rc.withFallbackCommand(fallbackCommandSuccessful)
		rc.withMaxBatchSize(20)
		rc.withBatchCommandCancelTimeout(100)
		rc.Start()

		params := []string{"test0", "test1", "test2", "test3", "test4"}
		results := getTestResults(params, rc)

		assertBatchSucceeded(t, results, len(params), "batch")
		fmt.Println("END 15")
	})

	t.Run("batch command timeout without fallback", func(t *testing.T) {
		batchFunction := func(ctx context.Context, params []*string) (map[string]*TestResult, error) {
			results := make(map[string]*TestResult)
			index := 0
			for _, param := range params {
				results[*param] = &TestResult{ID: param, Source: "test" + strconv.Itoa(index)}
				index++
			}
			return results, errors.New("timeout error")
		}

		rc, _ := NewRequestCollapser[TestResult, string](batchFunction, 10)
		rc.withMaxBatchSize(20)
		rc.withBatchCommandCancelTimeout(5)
		rc.Start()

		params := []string{"test0", "test1", "test2", "test3", "test4"}
		results := getTestResults(params, rc)

		assertBatchWithErrors(t, results, params)
		fmt.Println("END 16")
	})

	t.Run("batch command timeout but with success fallback", func(t *testing.T) {
		batchFunction := func(ctx context.Context, params []*string) (map[string]*TestResult, error) {
			results := make(map[string]*TestResult)
			index := 0
			for _, param := range params {
				id := *param
				results[*param] = &TestResult{ID: &id, Source: "batch"}
				index++
			}
			return results, errors.New("timeout error")
		}

		rc, _ := NewRequestCollapser[TestResult, string](batchFunction, 10)
		rc.withFallbackCommand(fallbackCommandSuccessful)
		rc.withMaxBatchSize(20)
		rc.withBatchCommandCancelTimeout(5)
		rc.Start()

		params := []string{"test0", "test1", "test2", "test3", "test4"}
		results := getTestResults(params, rc)

		assertBatchSucceeded(t, results, len(params), "fallback")
		fmt.Println("END 17")
	})

	t.Run("provided deep copy command fails but is not invoked since there are no duplicates", func(t *testing.T) {
		copyFunction := func(source *TestResult) (*TestResult, error) {
			return nil, errors.New("copy error")
		}

		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 20)
		rc.withMaxBatchSize(20)
		rc.withDeepCopyCommand(copyFunction)
		rc.Start()

		params := []string{"test0", "test1", "test2", "test3", "test4"}
		results := getTestResults(params, rc)

		assertBatchSucceeded(t, results, len(params), "batch")
		fmt.Println("END 18")
	})

	t.Run("provided deep copy command fails with no fallback", func(t *testing.T) {
		copyFunction := func(source *TestResult) (*TestResult, error) {
			return nil, errors.New("copy error")
		}

		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 20)
		rc.withMaxBatchSize(20)
		rc.withDeepCopyCommand(copyFunction)
		rc.Start()

		params := []string{"test0", "test0"}
		results := getTestResults(params, rc)

		oneSuccessful := false
		oneUnsuccessful := false
		for _, resultWithParam := range results {
			if resultWithParam.Error == nil {
				oneSuccessful = true
			} else {
				oneUnsuccessful = true
			}

		}
		assert.True(t, oneSuccessful)
		assert.True(t, oneUnsuccessful)
		assert.Equal(t, len(results), len(params))
		fmt.Println("END 19")
	})

	t.Run("provided deep copy command fails but fallback is successful", func(t *testing.T) {
		copyFunction := func(source *TestResult) (*TestResult, error) {
			return nil, errors.New("copy error")
		}

		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 20)
		rc.withFallbackCommand(fallbackCommandSuccessful)
		rc.withMaxBatchSize(20)
		rc.withDeepCopyCommand(copyFunction)
		rc.Start()

		params := []string{"test0", "test0", "test0"}
		results := getTestResults(params, rc)

		assertBatchSucceeded(t, results, len(params), "")
		fmt.Println("END 20")
	})

	t.Run("provided deep copy command is successful", func(t *testing.T) {
		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 20)
		rc.withFallbackCommand(fallbackCommandSuccessful)
		rc.withMaxBatchSize(20)
		rc.withDeepCopyCommand(copyCommandSuccessful)
		rc.Start()

		params := []string{"test0", "test0", "test0"}
		results := getTestResults(params, rc)

		assertBatchSucceeded(t, results, len(params), "batch")
		fmt.Println("END 21")
	})
	t.Run("get with timeout successful", func(t *testing.T) {
		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 20)
		rc.withMaxBatchSize(20)
		rc.Start()

		params := []string{"test0", "test1", "test2"}
		results := getTestResultsWithOperationalTimeout(params, rc, 50)

		assertBatchSucceeded(t, results, len(params), "batch")
		fmt.Println("END 22")
	})

	t.Run("get with timeout unsuccessful", func(t *testing.T) {
		batchFunction := func(ctx context.Context, params []*string) (map[string]*TestResult, error) {
			results := make(map[string]*TestResult)
			for _, param := range params {
				id := *param
				results[*param] = &TestResult{ID: &id, Source: "batch"}
				time.Sleep(100 * time.Millisecond)
			}
			return results, nil
		}
		rc, _ := NewRequestCollapser[TestResult, string](batchFunction, 20)
		rc.withMaxBatchSize(20)
		rc.Start()

		params := []string{"test0", "test1", "test2"}
		results := getTestResultsWithOperationalTimeout(params, rc, 50)

		assertBatchWithErrors(t, results, params)
		fmt.Println("END 23")
	})

	t.Run("caller context canceled", func(t *testing.T) {
		batchFunction := func(ctx context.Context, params []*string) (map[string]*TestResult, error) {
			results := make(map[string]*TestResult)
			for _, param := range params {
				id := *param
				results[*param] = &TestResult{ID: &id, Source: "batch"}
				time.Sleep(100 * time.Millisecond)
			}
			return results, nil
		}
		rc, _ := NewRequestCollapser[TestResult, string](batchFunction, 20)
		rc.withMaxBatchSize(20)
		rc.Start()

		params := []string{"test0", "test1", "test2"}

		results := getTestResultsWithContextTimeout(params, rc, 50)

		assertBatchWithErrors(t, results, params)
		fmt.Println("END 24")
	})

	t.Run("one with diagnostics", func(t *testing.T) {
		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 20)
		rc.withMaxBatchSize(20)
		rc.withDiagnosticEnabled(true)
		rc.Start()

		params := []string{"test0", "test1", "test2"}
		results := getTestResultsWithOperationalTimeout(params, rc, 50)

		assertBatchSucceeded(t, results, len(params), "batch")
		fmt.Println("END 25")
	})

	t.Run("stopping the collapser", func(t *testing.T) {
		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 20)
		rc.withMaxBatchSize(20)
		rc.withDiagnosticEnabled(true)
		rc.Start()
		defer rc.Stop()

		params := []string{"test0", "test1", "test2"}
		results := getTestResultsWithOperationalTimeout(params, rc, 50)

		assertBatchSucceeded(t, results, len(params), "batch")
		fmt.Println("END 26")
	})
}

func assertBatchWithErrors(t *testing.T, results []*TestResultWithParam, params []string) {
	for _, resultWithParam := range results {
		assert.Nil(t, resultWithParam.Result.ID)
		assert.Error(t, resultWithParam.Error)
		assert.Equal(t, resultWithParam.Result.Source, "")
	}
	assert.Equal(t, len(results), len(params))
}

func assertEmptyBatch(t *testing.T, results []*TestResultWithParam, params []string) {
	for _, resultWithParam := range results {
		assert.Nil(t, resultWithParam.Result.ID)
		assert.NoError(t, resultWithParam.Error)
		assert.Equal(t, resultWithParam.Result.Source, "")
	}
	assert.Equal(t, len(results), len(params))
}

func assertBatchSucceeded(t *testing.T, results []*TestResultWithParam, resultsCount int, source string) {
	resultPointers := make(map[string]bool, 0)
	resultValuePointers := make(map[string]bool, 0)
	for _, resultWithParam := range results {
		if source != "" { // is source == "" then there are mixed sources
			assert.Equal(t, resultWithParam.Result.Source, source)
		}
		assert.Equal(t, *resultWithParam.Result.ID, resultWithParam.Param)
		assert.NoError(t, resultWithParam.Error)
		// check that the result pointer is unique
		resultPointerAddress := fmt.Sprintf("%p", resultWithParam.Result)
		resultValuePointerAddress := fmt.Sprintf("%p", resultWithParam.Result.ID)
		_, ok1 := resultPointers[resultPointerAddress]
		_, ok2 := resultValuePointers[resultValuePointerAddress]
		assert.False(t, ok1)
		assert.False(t, ok2)
		resultPointers[resultPointerAddress] = true
		resultValuePointers[resultValuePointerAddress] = true
	}
	assert.Equal(t, len(results), resultsCount)
}

func getTestResults(params []string, rc *RequestCollapser[TestResult, string]) []*TestResultWithParam {
	return doGetTestResults(params, rc, 0, 0, 0)
}

func getTestResultsWithContextTimeout(params []string, rc *RequestCollapser[TestResult, string], contextTimeout time.Duration) []*TestResultWithParam {
	return doGetTestResults(params, rc, 0, contextTimeout, 0)
}

func getTestResultsWithOperationalTimeout(params []string, rc *RequestCollapser[TestResult, string], getOperationTimeout int64) []*TestResultWithParam {
	return doGetTestResults(params, rc, getOperationTimeout, 0, 0)
}

func getTestResultsWithTimeoutBetweenRequests(params []string, rc *RequestCollapser[TestResult, string], timeoutBetweenRequests time.Duration) []*TestResultWithParam {
	return doGetTestResults(params, rc, 0, 0, timeoutBetweenRequests)
}

func doGetTestResults(params []string, rc *RequestCollapser[TestResult, string], getOperationTimeout int64, contextTimeout, timeoutBetweenRequests time.Duration) []*TestResultWithParam {
	var resultsMutex = &sync.RWMutex{}
	results := make([]*TestResultWithParam, len(params))
	wg := sync.WaitGroup{}
	wg.Add(len(params))
	for i, param := range params {
		go func(index int, param string, resultsMutex *sync.RWMutex, results []*TestResultWithParam) {
			var result *TestResult
			var err error
			ctx := context.Background()
			if contextTimeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				time.AfterFunc(contextTimeout*time.Millisecond, cancel)
			}
			if getOperationTimeout > 0 {
				result, err = rc.GetWithTimeout(ctx, param, getOperationTimeout)
			} else {
				result, err = rc.Get(ctx, param)
			}
			resultsMutex.Lock()
			t := TestResultWithParam{}
			if result == nil {
				t.Result = &TestResult{}
				t.Result.ID = nil
				t.Result.Source = ""
				t.Error = err
			} else {
				t.Result = result
				t.Error = err
			}
			t.Param = param
			results[index] = &t
			resultsMutex.Unlock()
			wg.Done()
		}(i, param, resultsMutex, results)
		time.Sleep(timeoutBetweenRequests)
	}
	wg.Wait()
	return results
}
