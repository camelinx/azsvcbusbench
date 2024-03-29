package stats

import (
    "fmt"
    "context"
    "time"
    "sync"
    "sync/atomic"

    "github.com/golang/glog"
)

func NewStats( ids [ ]string, ctx context.Context )( stats *Stats ) {
    stats = &Stats{
        count  :    uint64( len( ids ) ),
        wg     :    &sync.WaitGroup{ },
    }

    stats.SetIds( ids )
    stats.SetCtx( ctx )

    return stats
}

func ( stats *Stats )SetIds( ids [ ]string )( err error ) {
    if nil == stats {
        return fmt.Errorf( "invalid stats context" )
    }

    stats.count = uint64( len( ids ) )
    stats.ids   = make( [ ]string, stats.count )
    copy( stats.ids, ids )

    stats.elems = make( [ ]statsElem, stats.count )
    for i, _ := range stats.elems {
        stats.elems[ i ].rcvdById = make( [ ]uint64, stats.count )
    }

    return nil
}

func ( stats *Stats )SetCtx( ctx context.Context ) {
    stats.ctx = ctx
}

func ( stats *Stats )SetStatsDumpInterval( intvl time.Duration ) {
    stats.dumpInterval = intvl
}

func ( stats *Stats )StartDumper( ) {
    stats.wg.Add( 1 )
    go func( ) {
        stats.dumpStats( )
        stats.wg.Done( )
    }( )
}

func ( stats *Stats )StopDumper( ) {
    stats.wg.Wait( )
}

func ( stats *Stats )UpdateSenderStat( idx int, incrBy uint64 ) {
    atomic.AddUint64( &stats.elems[ idx ].sent, incrBy )
}

func ( stats *Stats )UpdateReceiverStat( idx, fromIdx int, incrBy, lIncrBy uint64 ) {
    atomic.AddUint64( &stats.elems[ idx ].rcvd, incrBy )
    atomic.AddUint64( &stats.elems[ idx ].rcvdById[ fromIdx ], incrBy )
    atomic.AddUint64( &stats.elems[ idx ].latency, lIncrBy )

    // Not perfect but we can live with this
    maxLatency := stats.elems[ idx ].maxLatency
    if lIncrBy > maxLatency {
        atomic.CompareAndSwapUint64( &stats.elems[ idx ].maxLatency, maxLatency, lIncrBy )
    }
}

func ( stats *Stats )UpdateReceiverStatRetries( idx int, retries uint64 ) {
    atomic.AddUint64( &stats.elems[ idx ].retries, retries )

    maxRetries := stats.elems[ idx ].maxRetries
    if retries > maxRetries {
        atomic.CompareAndSwapUint64( &stats.elems[ idx ].maxRetries, maxRetries, retries )
    }
}

func ( stats *Stats )UpdateReceiverStatErrors( idx int, errors uint64 ) {
    atomic.AddUint64( &stats.elems[ idx ].errors, errors )
}

func ( stats *Stats )dumpStats( ) {
    ticker := time.NewTicker( stats.dumpInterval )

    for {
        select {
            case <-stats.ctx.Done( ):
                stats.dump( true )
                return

            case <-ticker.C:
                stats.dump( false )
        }
    }
}

func ( stats *Stats )dump( byId bool ) {
    glog.Infof( "---" )
    for i, v := range stats.elems {
        avgLatency := uint64( 0 )
        if v.rcvd > 0 {
            avgLatency = v.latency / v.rcvd
        }

        fmt.Printf(
            "%v: Sent %v Rcvd %v Retries %v Max Retries %v Avg Latency %v Max Latency %v Errors %v\n",
            stats.ids[ i ], v.sent, v.rcvd, v.retries, v.maxRetries, avgLatency, v.maxLatency, v.errors,
        )

        if byId {
            for j, jv := range v.rcvdById {
                fmt.Printf( "%v: Received %v\n", stats.ids[ j ], jv )
            }
        }
    }
    glog.Infof( "---" )
}
