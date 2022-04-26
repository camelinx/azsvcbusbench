package helpers

import (
    "testing"
    "strings"
    "io"

    "github.com/google/uuid"
)

const (
    idGenMagicNum   =   32
    idReaderStr     =   "abcd-1234\nefgh-5678\nijkl-9012"
)

func testNewIdGenerator( t *testing.T )( idGen *IdGen ) {
    idGen = NewIdGenerator( )
    if nil == idGen {
        t.Fatalf( "NewIdGenerator - failed to initialize" )
    }

    return idGen
}

func getIdReader( )( io.Reader ) {
    var idStr string

    for i := 0; i < idGenMagicNum; i++ {
        idStr += uuid.New( ).String( ) + "\n"
    }

    return strings.NewReader( idStr )
}

func TestInitIdBlockFromReader( t *testing.T ) {
    idGen := testNewIdGenerator( t )
    err   := idGen.InitIdBlockFromReader( getIdReader( ) )
    if err != nil || !idGen.Initialized {
        t.Fatalf( "InitIdBlockFromReader - failed to initialize, error %v", err )
    }

    err = idGen.InitIdBlock( 0 )
    if err != nil || !idGen.Initialized {
        t.Fatalf( "InitIdBlock - failed to detect earlier initialization from reader" )
    }

    idGen = testNewIdGenerator( t )
    err   = idGen.InitIdBlockFromReader( nil )
    if err == nil {
        t.Fatalf( "InitIdBlockFromReader - successfully initialized from nil reader" )
    }
}

func TestInitIdBlock( t *testing.T ) {
    idGen := testNewIdGenerator( t )
    err   := idGen.InitIdBlock( idGenMagicNum )
    if err != nil || !idGen.Initialized {
        t.Fatalf( "InitIdBlock - failed to initialize with count %v, error %v", idGenMagicNum, err )
    }

    if idGenMagicNum != idGen.Count {
        t.Fatalf( "InitIdBlock - count mismatch expected %v saw %v", idGenMagicNum, idGen.Count )
    }

    for i := 0; i < idGenMagicNum; i++ {
        if len( idGen.Block[ i ] ) == 0 {
            t.Fatalf( "InitIdBlock - empty id string found" )
        }
    }

    err = idGen.InitIdBlockFromReader( nil )
    if err != nil || !idGen.Initialized {
        t.Fatalf( "InitIdBlock - failed to detect earlier initialization from count" )
    }
}
