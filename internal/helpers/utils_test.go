package helpers

import (
    "testing"
    "time"
    "math/rand"
    "encoding/json"
)

func TestGetRandomString( t *testing.T ) {
    rand.Seed( time.Now( ).UnixNano( ) )

    rstrLen := rand.Intn( 256 )
    rstr    := GetRandomString( uint( rstrLen ) )
    if len( rstr ) != rstrLen {
        t.Errorf( "GetRandomString - failed to return random string of length %v", rstrLen )
    }
}

func TestGetRandomJsonString( t *testing.T ) {
    rand.Seed( time.Now( ).UnixNano( ) )

    for i := 0; i < 1; i++ {
        jsonStrLen      := rand.Intn( 1024 )
        jsonStr, retLen := GetRandomJsonString( uint( jsonStrLen ) )

        if len( jsonStr ) != int( retLen ) || int( retLen ) < jsonStrLen {
            t.Errorf( "GetRandomJsonString - json string is shorter than request length %v", jsonStrLen )
        }

        if !json.Valid( [ ]byte( jsonStr ) ) {
            t.Errorf( "GetRandomJsonString - invalid json string returned" )
            t.Logf( "%v", jsonStr )
        }
    }
}
