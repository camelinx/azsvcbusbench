package stats

import (
    "context"
    "sync"
    "time"
)

type statsElem struct {
    sent             uint64

    rcvd             uint64
    rcvdById      [ ]uint64

    retries          uint64
    maxRetries       uint64

    latency          uint64
    maxLatency       uint64

    errors           uint64
}

type Stats struct {
    ids           [ ]string
    elems         [ ]statsElem
    count            uint64
    ctx              context.Context
    wg              *sync.WaitGroup
    dumpInterval     time.Duration
}
