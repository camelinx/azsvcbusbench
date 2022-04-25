package stats

import (
    "context"
    "sync"
)

type statsElem struct {
    sent        uint64 // Number of messages sent
    rcvd        uint64 // Number of messages received
    rcvdById [ ]uint64 // Received per id 
    latency     uint64 // Cumulative latency
}

type Stats struct {
    ids     [ ]string
    elems   [ ]statsElem
    count      uint64
    ctx        context.Context
    wg        *sync.WaitGroup
}
