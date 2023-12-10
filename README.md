# Request Collapser

## Overview

The `requestcollapser` package provides a mechanism to collapse multiple requests into one batch request. This can be useful for optimizing and reducing the number of calls made to an external service.

## Types

### `collapserRequest[T any, P comparable]`

Represents a request to be collapsed, containing the parameter (`param`) and a channel (`resultChannel`) to receive the response.

### `collapserResponse[T any, P comparable]`

Represents the response for a collapsed request, containing the parameter (`param`), the result (`result`), and any potential error (`err`).

### `RequestCollapser[T any, P comparable]`

The main struct that encapsulates the functionality of collapsing requests. It has various configuration options and methods for managing the collapser.

## Constants

- `MAX_BATCH_TIMEOUT`: The maximum timeout for the batch command.
- `MAX_QUEUE_SIZE`: The maximum queue size for requests to be batched.

## Methods

### Configuration Methods

- `WithFallbackCommand`: Provides the fallback command to be executed in case of batch command failure.
- `WithDeepCopyCommand`: Provides the command to be executed to deep copy the result of the batch command.
- `WithMaxBatchSize`: Provides the limit of requests to be batched together before triggering the batch command.
- `WithBatchCommandTimeout`: Provides the max time to wait for the batch command to complete.
- `WithAllowDuplicatedParams`: Allows or disallows duplicated parameters in the batch.
- `WithDiagnosticEnabled`: Enables or disables diagnostics logging.

### Constructor Method

- `NewRequestCollapser`: Creates a new `RequestCollapser` instance with the specified batch command and interval.

### Lifecycle Methods

- `Start`: Starts the collapser, initiating the request acceptor, request processor ticker, and request processor.
- `Stop`: Stops the collapser, closing channels and notifying goroutines to terminate.

### Request Methods

- `Get`: Sends a request to the collapser and waits for the result.
- `GetWithTimeout`: Sends a request to the collapser and waits for the result with a specified timeout.
- `QueueRequest`: Queues a request to be processed asynchronously.

## Example Usage

```go
package main

import (
	"context"
	"fmt"
	"time"
)

func main() {
	// Define a batch command
	batchCommand := func(ctx context.Context, params []*int) (map[int]*string, error) {
		// Implementation of the batch command
		// ...
		return nil, nil
	}

	// Create a new RequestCollapser instance
	collapser, err := requestcollapser.NewRequestCollapser(batchCommand, 100)
	if err != nil {
		fmt.Println("Error creating RequestCollapser:", err)
		return
	}

	// Start the collapser
	collapser.Start()

	// Send requests to the collapser
	result, err := collapser.Get(context.Background(), 42)
	if err != nil {
		fmt.Println("Error getting result:", err)
		return
	}

	// Stop the collapser
	collapser.Stop()

	fmt.Println("Result:", result)
}
