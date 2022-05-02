package main

import (
    "flag"
    "os"

    "github.com/golang/glog"
    "github.com/azsvcbusbench/internal/helpers"
)

var (
    count       = flag.Int( "count", 256, "Number of ip addresses to generate" )
    file        = flag.String( "file", "", "File to write generated ip addresses to" )
)

func main( ) {
    flag.Parse( )

    err := flag.Lookup( "logtostderr" ).Value.Set( "true" )
    if err != nil {
        glog.Fatalf( "Error setting logtostderr to true: %v", err )
    }

    glog.Infof( "Starting ipv4gen" )

    ipv4Gen := helpers.NewIpv4Generator( )
    err    = ipv4Gen.InitIpv4Block( *count, helpers.Ipv4AddrClassAny )
    if err != nil && !ipv4Gen.Initialized {
        glog.Fatalf( "Error generating ip addresses: %v", err )
    }

    var fh *os.File

    if len( *file ) > 0 {
        fh, err = os.Create( *file )
        if err != nil {
            glog.Fatalf( "Failed to create/open file %v: %v", *file, err )
        }

        defer fh.Close( )
    }

    for _, ip := range ipv4Gen.Block {
        if fh != nil {
            _, err = fh.WriteString( ip + "\n" )
            if err != nil {
                glog.Fatalf( "Failed to write to file %v: %v", *file, err )
            }
        }

        glog.Infof( "%v", ip )
    }
}
