package helpers

import (
    "bufio"
    "io"
    "os"
    "fmt"
)

type iocb func( string )( error )

func ReadFile( file string, cb iocb )( err error ) {
    fh, err := os.Open( file )
    if err != nil {
        return err
    }

    defer func( ) {
        fh.Close( )
    }( )

    return ProcessFile( fh, cb )
}

func ProcessFile( fh io.Reader, cb iocb )( err error ) {
    if nil == fh {
        return fmt.Errorf( "invalid io reader" )
    }

    if nil == cb {
        return nil
    }

    scanner := bufio.NewScanner( fh )
    for scanner.Scan( ) {
        err = cb( scanner.Text( ) )
        if err != nil {
            return err
        }
    }

    if err := scanner.Err( ); err != nil {
        return err
    }

    return nil
}
