package main

import (
    "flag"
    "os"

    "github.com/golang/glog"
    "github.com/azsvcbusbench/internal/helpers"
)

var (
    count       = flag.Int( "count", 128, "Number of ids to generate" )
    file        = flag.String( "file", "", "File to write generated ids to" )
)

func main( ) {
    flag.Parse( )

    err := flag.Lookup( "logtostderr" ).Value.Set( "true" )
    if err != nil {
        glog.Fatalf( "Error setting logtostderr to true: %v", err )
    }

    glog.Infof( "Starting idgen" )

    idGen := helpers.NewIdGenerator( )
    err    = idGen.InitIdBlock( *count )
    if err != nil {
        glog.Fatalf( "Error generating ids: %v", err )
    }

    var fh *os.File

    if len( *file ) > 0 {
        fh, err = os.Create( *file )
        if err != nil {
            glog.Fatalf( "Failed to create/open file %v: %v", *file, err )
        }

        defer fh.Close( )
    }

    for _, id := range idGen.Block {
        if fh != nil {
            _, err = fh.WriteString( id + "\n" )
            if err != nil {
                glog.Fatalf( "Failed to write to file %v: %v", *file, err )
            }
        }

        glog.Infof( "%v", id )
    }
}
