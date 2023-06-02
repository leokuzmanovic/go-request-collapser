# go-request-collapser
Inspired by Netflix Hystrix Collapser, this is a stateless library writen in Go that facilitates collapsing multiple requests into a single batch call based on 
the configured time frame or max batch size.

Collapser is instantiated by providing the batch function and time interval at which it would be invoked. 
It is also possible to provide a fallback function that would be invoked in case the batch command returns an error. 

For example, if we would have a function that provides Movie data based on some id:
```
getSingle = func (ctx context.Context, id *string) (*Movie, error) {
  movie, err := fetchMovieFromDB(ctx, id)
  if err != nil {
    return nil, err
  }
  return &Movie{Genre: movie.Genre, Title: movie.Title}, nil
}
```
And if this function would be invoked a lot of times in a short period of time (with or without repeating the ids),
we would need to execute a lot of DB queries to get all of those movie responses.

What we want is to have the ability to batch these requests and do one batch call to the DB after which the results (if existing) would be passed back to callers.

This is where RequestCollapser comes to play!

First we need to create a batch function:
```
getBatch = func(ctx context.Context, ids []*string) (map[string]*Movie, error) {
    results := make(map[string]*Movie)
    movies, err := fetchMoviesFromDB(ctx, ids)
    for _, movie := range movies {
	results[movie.Id] = &Movie{Genre: movie.Genre, Title: movie.Title}
    }
    return results, nil
}
```

Then we define a request collapser with the batch function and interval in milliseconds: 
```
rc, err := NewRequestCollapser[Movie, string](getBatch, 20)
// handle err
// add optional RequestCollapser configuration here...
```

and call Start() to start collapser routines which will call the batch function every 20 milliseconds if there were any collected requests:
```
rc.Start()
```

Then, instead of 'getSingle' function, we use the collapser to get the results:
```
movie, err = rc.Get(ctx, movieId)
```

Optionally, we could add additional configuration to the collapser:
```
rc.WithFallbackCommand(getSingle) // Provides the fallback command to be executed in case of batch command failure 
rc.WithMaxBatchSize(100) // Provides the limit of requests to be batched together before triggering the batch command
rc.WithBatchCommandTimeout(0) // Provides the max time to wait for the batch command to complete
rc.WithDeepCopyCommand(copyFunction) // Provides the command to be executed to deep copy the result of batch command
rc.WithDiagnosticEnabled(true) // Provides the diagnostics flag. If set, RequestCollapser logs will be printed to stdout
```




  
