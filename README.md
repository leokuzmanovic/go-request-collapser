# go-request-collapser
Inspired by Netflix Hystrix Collapser, this is a stateless library writen in Go that facilitates collapsing multiple requests into a single batch call based on configured time frame or max batch size.

Collapser is instantiated by providing the batch function and time interval at which it would be invoked. 
It is also possible to provide a fallback function that would be invoked in case the batch command returns an error. 
