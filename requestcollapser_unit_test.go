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

	deepCopyCommandSuccessful = func(original *TestResult) (*TestResult, error) {
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
	})

	t.Run("invalid batch command interval", func(t *testing.T) {
		rc, err := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 0)
		assert.Nil(t, rc)
		assert.Error(t, err)
	})

	t.Run("get one result", func(t *testing.T) {
		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 20)
		_ = rc.WithFallbackCommand(fallbackCommandSuccessful)
		_ = rc.WithMaxBatchSize(20)
		_ = rc.WithBatchCommandTimeout(0)
		rc.Start()
		defer rc.Stop()

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
	})

	t.Run("get multiple results", func(t *testing.T) {
		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 20)
		_ = rc.WithFallbackCommand(fallbackCommandSuccessful)
		_ = rc.WithMaxBatchSize(20)
		_ = rc.WithBatchCommandTimeout(0)
		rc.Start()
		defer rc.Stop()

		params := []string{"test0", "test1", "test2", "test3", "test4", "test5", "test6", "test7", "test8", "test9"}
		results := getTestResults(params, rc)

		assertBatchSucceeded(t, results, len(params), "batch")
	})

	t.Run("duplicated params not allowed", func(t *testing.T) {
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
		_ = rc.WithFallbackCommand(fallbackCommandSuccessful)
		_ = rc.WithMaxBatchSize(20)
		_ = rc.WithBatchCommandTimeout(0)
		rc.Start()
		defer rc.Stop()

		params := []string{"test0", "test1", "test2", "test2", "test3", "test3", "test3", "test4", "test4", "test4", "test4"}
		results := getTestResults(params, rc)

		assertBatchSucceeded(t, results, len(params), "batch")
		assert.Equal(t, len(batchParamArgs), 5)
	})

	t.Run("duplicated params allowed", func(t *testing.T) {
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
		_ = rc.WithFallbackCommand(fallbackCommandSuccessful)
		_ = rc.WithMaxBatchSize(20)
		_ = rc.WithBatchCommandTimeout(0)
		_ = rc.WithAllowDuplicatedParams(true)
		rc.Start()
		defer rc.Stop()

		params := []string{"test0", "test1", "test2", "test2", "test3", "test3", "test3", "test4", "test4", "test4", "test4"}
		results := getTestResults(params, rc)

		assertBatchSucceeded(t, results, len(params), "batch")
		assert.Equal(t, len(batchParamArgs), 11)
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
		_ = rc.WithFallbackCommand(fallbackCommandSuccessful)
		_ = rc.WithMaxBatchSize(20)
		_ = rc.WithBatchCommandTimeout(0)
		rc.Start()
		defer rc.Stop()

		params := []string{"test0", "test1", "test2", "test0", "test1", "test2", "test6", "test7", "test8", "test9"}
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
	})

	t.Run("batch returns empty results", func(t *testing.T) {
		batchFunction := func(ctx context.Context, params []*string) (map[string]*TestResult, error) {
			return make(map[string]*TestResult), nil
		}

		rc, _ := NewRequestCollapser[TestResult, string](batchFunction, 20)
		_ = rc.WithFallbackCommand(fallbackCommandSuccessful)
		_ = rc.WithMaxBatchSize(20)
		_ = rc.WithBatchCommandTimeout(0)
		rc.Start()
		defer rc.Stop()

		params := []string{"test0", "test1", "test2", "test3", "test4", "test5", "test6", "test7", "test8", "test9"}
		results := getTestResults(params, rc)

		assertEmptyBatch(t, results, params)
	})

	t.Run("batch return nil results", func(t *testing.T) {
		batchFunction := func(ctx context.Context, params []*string) (map[string]*TestResult, error) {
			return nil, nil
		}

		rc, _ := NewRequestCollapser[TestResult, string](batchFunction, 20)
		_ = rc.WithFallbackCommand(fallbackCommandSuccessful)
		_ = rc.WithMaxBatchSize(20)
		_ = rc.WithBatchCommandTimeout(0)
		rc.Start()
		defer rc.Stop()

		params := []string{"test0", "test1", "test2", "test3", "test4", "test5", "test6", "test7", "test8", "test9"}
		results := getTestResults(params, rc)

		assertEmptyBatch(t, results, params)
	})

	t.Run("batch returns error and no fallback provided", func(t *testing.T) {
		batchFunction := func(ctx context.Context, params []*string) (map[string]*TestResult, error) {
			return nil, errors.New("batch error")
		}

		rc, _ := NewRequestCollapser[TestResult, string](batchFunction, 20)
		_ = rc.WithMaxBatchSize(20)
		_ = rc.WithBatchCommandTimeout(0)
		rc.Start()
		defer rc.Stop()

		params := []string{"test0", "test1", "test2", "test3", "test4", "test5", "test6", "test7", "test8", "test9"}
		results := getTestResults(params, rc)

		assertBatchWithErrors(t, results, params)
	})

	t.Run("batch returns error and so does fallback", func(t *testing.T) {
		batchFunction := func(ctx context.Context, params []*string) (map[string]*TestResult, error) {
			return nil, errors.New("batch error")
		}
		fallbackFunction := func(ctx context.Context, params *string) (*TestResult, error) {
			return nil, errors.New("batch error")
		}

		rc, _ := NewRequestCollapser[TestResult, string](batchFunction, 20)
		_ = rc.WithFallbackCommand(fallbackFunction)
		_ = rc.WithMaxBatchSize(20)
		_ = rc.WithBatchCommandTimeout(0)
		rc.Start()
		defer rc.Stop()

		params := []string{"test0", "test1", "test2"}
		results := getTestResults(params, rc)

		assertBatchWithErrors(t, results, params)
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
		_ = rc.WithFallbackCommand(fallbackFunction)
		_ = rc.WithMaxBatchSize(20)
		_ = rc.WithBatchCommandTimeout(0)
		rc.Start()
		defer rc.Stop()

		params := []string{"test0", "test1", "test2"}
		results := getTestResults(params, rc)

		assertBatchSucceeded(t, results, len(params), "fallback")
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
		_ = rc.WithFallbackCommand(fallbackFunction)
		_ = rc.WithMaxBatchSize(20)
		_ = rc.WithBatchCommandTimeout(0)
		rc.Start()
		defer rc.Stop()

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
	})

	t.Run("collapser ticker ticks before all requests are aggregated", func(t *testing.T) {
		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 10)
		_ = rc.WithFallbackCommand(fallbackCommandSuccessful)
		_ = rc.WithMaxBatchSize(20)
		_ = rc.WithBatchCommandTimeout(0)
		rc.Start()
		defer rc.Stop()

		params := []string{"test0", "test1", "test2", "test3"}
		results := getTestResultsWithTimeoutBetweenRequests(params, rc, 10*time.Millisecond)

		assertBatchSucceeded(t, results, len(params), "batch")
	})

	t.Run("collapser buffer gets full before the ticker", func(t *testing.T) {
		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 100)
		_ = rc.WithFallbackCommand(fallbackCommandSuccessful)
		_ = rc.WithMaxBatchSize(4)
		_ = rc.WithBatchCommandTimeout(0)
		rc.Start()
		defer rc.Stop()

		params := []string{"test0", "test1", "test2", "test3", "test4"}
		results := getTestResults(params, rc)

		assertBatchSucceeded(t, results, len(params), "batch")
	})

	t.Run("batch command is faster than the batch timeout", func(t *testing.T) {
		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 20)
		_ = rc.WithFallbackCommand(fallbackCommandSuccessful)
		_ = rc.WithMaxBatchSize(20)
		_ = rc.WithBatchCommandTimeout(100)
		rc.Start()
		defer rc.Stop()

		params := []string{"test0", "test1", "test2", "test3", "test4"}
		results := getTestResults(params, rc)

		assertBatchSucceeded(t, results, len(params), "batch")
	})

	t.Run("batch command timeout without fallback", func(t *testing.T) {
		batchFunction := func(ctx context.Context, params []*string) (map[string]*TestResult, error) {
			results := make(map[string]*TestResult)
			index := 0
			for _, param := range params {
				results[*param] = &TestResult{ID: param, Source: "test" + strconv.Itoa(index)}
				index++
			}
			time.Sleep(100 * time.Millisecond)
			return results, nil
		}

		rc, _ := NewRequestCollapser[TestResult, string](batchFunction, 10)
		_ = rc.WithMaxBatchSize(20)
		_ = rc.WithBatchCommandTimeout(5)
		rc.Start()
		defer rc.Stop()

		params := []string{"test0", "test1", "test2", "test3", "test4"}
		results := getTestResults(params, rc)

		assertBatchWithErrors(t, results, params)
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
		_ = rc.WithFallbackCommand(fallbackCommandSuccessful)
		_ = rc.WithMaxBatchSize(20)
		_ = rc.WithBatchCommandTimeout(5)
		rc.Start()
		defer rc.Stop()

		params := []string{"test0", "test1", "test2", "test3", "test4"}
		results := getTestResults(params, rc)

		assertBatchSucceeded(t, results, len(params), "fallback")
	})

	t.Run("provided deep copy command fails but is not invoked since there are no duplicates", func(t *testing.T) {
		copyFunction := func(source *TestResult) (*TestResult, error) {
			return nil, errors.New("copy error")
		}

		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 20)
		_ = rc.WithMaxBatchSize(20)
		_ = rc.WithDeepCopyCommand(copyFunction)
		rc.Start()
		defer rc.Stop()

		params := []string{"test0", "test1", "test2", "test3", "test4"}
		results := getTestResults(params, rc)

		assertBatchSucceeded(t, results, len(params), "batch")
	})

	t.Run("provided deep copy command fails with no fallback", func(t *testing.T) {
		copyFunction := func(source *TestResult) (*TestResult, error) {
			return nil, errors.New("copy error")
		}

		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 20)
		_ = rc.WithMaxBatchSize(20)
		_ = rc.WithDeepCopyCommand(copyFunction)
		rc.Start()
		defer rc.Stop()

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
	})

	t.Run("provided deep copy command fails but fallback is successful", func(t *testing.T) {
		copyFunction := func(source *TestResult) (*TestResult, error) {
			return nil, errors.New("copy error")
		}

		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 20)
		_ = rc.WithFallbackCommand(fallbackCommandSuccessful)
		_ = rc.WithMaxBatchSize(20)
		_ = rc.WithDeepCopyCommand(copyFunction)
		rc.Start()
		defer rc.Stop()

		params := []string{"test0", "test0", "test0"}
		results := getTestResults(params, rc)

		assertBatchSucceeded(t, results, len(params), "")
	})

	t.Run("provided deep copy command is successful", func(t *testing.T) {
		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 20)
		_ = rc.WithFallbackCommand(fallbackCommandSuccessful)
		_ = rc.WithMaxBatchSize(20)
		_ = rc.WithDeepCopyCommand(deepCopyCommandSuccessful)
		rc.Start()

		params := []string{"test0", "test0", "test0"}
		results := getTestResults(params, rc)

		assertBatchSucceeded(t, results, len(params), "batch")
	})

	t.Run("get with timeout successful", func(t *testing.T) {
		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 20)
		_ = rc.WithMaxBatchSize(20)
		rc.Start()
		defer rc.Stop()

		params := []string{"test0", "test1", "test2"}
		results := getTestResultsWithOperationalTimeout(params, rc, 50)

		assertBatchSucceeded(t, results, len(params), "batch")
	})

	t.Run("get with invalid timeout", func(t *testing.T) {
		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 20)
		_ = rc.WithMaxBatchSize(20)
		rc.Start()
		defer rc.Stop()

		params := []string{"test0"}
		results := getTestResultsWithOperationalTimeout(params, rc, 0)
		assertBatchWithErrors(t, results, params)
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
		_ = rc.WithMaxBatchSize(20)
		rc.Start()
		defer rc.Stop()

		params := []string{"test0", "test1", "test2"}
		results := getTestResultsWithOperationalTimeout(params, rc, 50)

		assertBatchWithErrors(t, results, params)
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
		_ = rc.WithMaxBatchSize(20)
		rc.Start()
		defer rc.Stop()

		params := []string{"test0", "test1", "test2"}

		results := getTestResultsWithContextTimeout(params, rc, 50)

		assertBatchWithErrors(t, results, params)
	})

	t.Run("one with diagnostics", func(t *testing.T) {
		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 20)
		_ = rc.WithMaxBatchSize(20)
		_ = rc.WithDiagnosticEnabled(true)
		rc.Start()
		defer rc.Stop()

		params := []string{"test0", "test1", "test2"}
		results := getTestResultsWithOperationalTimeout(params, rc, 50)

		assertBatchSucceeded(t, results, len(params), "batch")
	})

	t.Run("mixed operational timeouts but with fallback", func(t *testing.T) {
		batchCommand := func(ctx context.Context, params []*string) (map[string]*TestResult, error) {
			results := make(map[string]*TestResult)
			for _, param := range params {
				id := *param
				results[*param] = &TestResult{ID: &id, Source: "batch"}
			}
			time.Sleep(30 * time.Millisecond)
			return results, nil
		}

		rc, _ := NewRequestCollapser[TestResult, string](batchCommand, 500)
		_ = rc.WithMaxBatchSize(2)
		_ = rc.WithFallbackCommand(fallbackCommandSuccessful)
		rc.Start()
		defer rc.Stop()

		params := []string{"test0", "test1", "test2", "test3"}
		results := getTestResultsWithOperationalTimeouts(params, rc, []int64{100, 100, 10, 10})

		for _, resultWithParam := range results {
			if resultWithParam.Param == "test0" || resultWithParam.Param == "test1" {
				assert.Equal(t, resultWithParam.Result.Source, "batch")
			} else {
				assert.Equal(t, resultWithParam.Result.Source, "fallback")
			}
			assert.Equal(t, *resultWithParam.Result.ID, resultWithParam.Param)
			assert.NoError(t, resultWithParam.Error)
		}
		assert.Equal(t, len(results), 4)
	})

	t.Run("stopping the collapser", func(t *testing.T) {
		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 20)
		_ = rc.WithMaxBatchSize(20)
		_ = rc.WithDiagnosticEnabled(true)
		rc.Start()
		defer rc.Stop()

		params := []string{"test0", "test1", "test2"}
		results := getTestResultsWithOperationalTimeout(params, rc, 50)

		assertBatchSucceeded(t, results, len(params), "batch")
	})

	t.Run("get one on stopped collapser", func(t *testing.T) {
		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 20)
		rc.Start()
		rc.Stop()

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

		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("queue one", func(t *testing.T) {
		batchFunction := func(ctx context.Context, params []*string) (map[string]*NO_RESULT, error) {
			return nil, nil
		}
		rc, _ := NewRequestCollapser[NO_RESULT, string](batchFunction, 20)
		rc.Start()
		defer rc.Stop()

		var err error

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			err = rc.QueueRequest("param1")
			wg.Done()
		}()
		wg.Wait()

		assert.NoError(t, err)
	})

	t.Run("queue one on stopped collapser", func(t *testing.T) {
		batchFunction := func(ctx context.Context, params []*string) (map[string]*NO_RESULT, error) {
			return nil, nil
		}
		rc, _ := NewRequestCollapser[NO_RESULT, string](batchFunction, 20)
		rc.Start()
		rc.Stop()

		var err error
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			err = rc.QueueRequest("param1")
			wg.Done()
		}()
		wg.Wait()

		assert.Error(t, err)
	})

	t.Run("try modifications when started", func(t *testing.T) {
		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 20)
		rc.Start()

		err := rc.WithDiagnosticEnabled(true)
		assert.Error(t, err)
		err = rc.WithMaxBatchSize(1)
		assert.Error(t, err)
		err = rc.WithAllowDuplicatedParams(true)
		assert.Error(t, err)
		err = rc.WithFallbackCommand(fallbackCommandSuccessful)
		assert.Error(t, err)
		err = rc.WithBatchCommandTimeout(1)
		assert.Error(t, err)
		err = rc.WithDeepCopyCommand(deepCopyCommandSuccessful)
		assert.Error(t, err)
	})

	t.Run("try modifications when started", func(t *testing.T) {
		rc, _ := NewRequestCollapser[TestResult, string](batchCommandSuccessful, 20)
		rc.Start()
		rc.Stop()

		err := rc.WithDiagnosticEnabled(true)
		assert.NoError(t, err)
		err = rc.WithMaxBatchSize(1)
		assert.NoError(t, err)
		err = rc.WithAllowDuplicatedParams(true)
		assert.NoError(t, err)
		err = rc.WithFallbackCommand(fallbackCommandSuccessful)
		assert.NoError(t, err)
		err = rc.WithBatchCommandTimeout(1)
		assert.NoError(t, err)
		err = rc.WithDeepCopyCommand(deepCopyCommandSuccessful)
		assert.NoError(t, err)
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
	return doGetTestResults(params, rc, make([]int64, 0), 0, 0)
}

func getTestResultsWithContextTimeout(params []string, rc *RequestCollapser[TestResult, string], contextTimeout time.Duration) []*TestResultWithParam {
	return doGetTestResults(params, rc, make([]int64, 0), contextTimeout, 0)
}

func getTestResultsWithOperationalTimeout(params []string, rc *RequestCollapser[TestResult, string], getOperationTimeout int64) []*TestResultWithParam {
	getOperationTimeouts := make([]int64, len(params))
	for i := range getOperationTimeouts {
		getOperationTimeouts[i] = getOperationTimeout
	}
	return doGetTestResults(params, rc, getOperationTimeouts, 0, 0)
}

func getTestResultsWithOperationalTimeouts(params []string, rc *RequestCollapser[TestResult, string], getOperationTimeouts []int64) []*TestResultWithParam {
	return doGetTestResults(params, rc, getOperationTimeouts, 0, 0)
}

func getTestResultsWithTimeoutBetweenRequests(params []string, rc *RequestCollapser[TestResult, string], timeoutBetweenRequests time.Duration) []*TestResultWithParam {
	return doGetTestResults(params, rc, make([]int64, 0), 0, timeoutBetweenRequests)
}

func doGetTestResults(params []string, rc *RequestCollapser[TestResult, string], getOperationTimeouts []int64,
	contextTimeout, timeoutBetweenRequests time.Duration) []*TestResultWithParam {
	var resultsMutex = &sync.RWMutex{}
	results := make([]*TestResultWithParam, len(params))
	wg := sync.WaitGroup{}
	wg.Add(len(params))
	for i, param := range params {
		go func(index int, param string, resultsMutex *sync.RWMutex, results *[]*TestResultWithParam) {
			var result *TestResult
			var err error
			ctx := context.Background()
			if contextTimeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				time.AfterFunc(contextTimeout*time.Millisecond, cancel)
			}
			if len(getOperationTimeouts) > 0 {
				result, err = rc.GetWithTimeout(ctx, param, getOperationTimeouts[index])
			} else {
				result, err = rc.Get(ctx, param)
			}
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
			resultsMutex.Lock()
			(*results)[index] = &t
			resultsMutex.Unlock()
			wg.Done()
		}(i, param, resultsMutex, &results)
		time.Sleep(timeoutBetweenRequests)
	}
	wg.Wait()
	return results
}
