package helpers

import (
    "io"
    "fmt"

    "github.com/google/uuid"
)

type IdGen struct {
    Block       [ ]string
    Count          int
    Initialized    bool
}

func NewIdGenerator( )( *IdGen ) {
    return &IdGen{ }
}

func ( idGen *IdGen )InitIdBlockFromReader( file io.Reader )( err error ) {
    if idGen.Initialized {
        return nil
    }

    cb := func ( id string )( error ) {
        idGen.Block = append( idGen.Block, id )
        idGen.Count++
        return nil
    }

    err = ProcessFile( file, cb )
    if err != nil {
        return err
    }

    idGen.Initialized = true
    return nil
}

func ( idGen *IdGen )InitIdBlock( blockCount int )( err error ) {
    if idGen.Initialized {
        return nil
    }

    if blockCount == 0 {
        return fmt.Errorf( "id count is 0" )
    }

    idGen.Count = blockCount
    idGen.Block = make( [ ]string, idGen.Count )

    for i := 0; i < idGen.Count; i++ {
        idGen.Block[ i ] = uuid.New( ).String( )
    }

    idGen.Initialized = true
    return nil
}
