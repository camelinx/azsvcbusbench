package helpers

import (
    "fmt"
    "testing"
    "strings"
)

func errCb( line string )( err error ) {
    return fmt.Errorf( "this is an error" )
}

func regCb( line string )( err error ) {
    return nil
}

func TestProcessFile( t *testing.T ) {
    strReader := strings.NewReader( "Line1\nLine2\nLine3" )
    err := ProcessFile( strReader, regCb )
    if err != nil {
        t.Errorf( "ProcessFile - failed regular test" )
    }

    strReader = strings.NewReader( "Line1\nLine2\nLine3" )
    err = ProcessFile( strReader, errCb )
    if err == nil {
        t.Errorf( "ProcessFile - failed to handle error from callback" )
    }

    strReader = strings.NewReader( "Line1\nLine2\nLine3" )
    err = ProcessFile( strReader, nil )
    if err != nil {
        t.Errorf( "ProcessFile - returned error in case of nil callback" )
    }

    err = ProcessFile( nil, regCb )
    if err == nil {
        t.Errorf( "ProcessFile - failed to handle invalid io reader" )
    }
}
