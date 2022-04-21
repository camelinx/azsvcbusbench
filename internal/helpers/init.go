package helpers

import (
    "time"
    "math/rand"
)

func init( ) {
    rand.Seed( time.Now( ).UnixNano( ) )
}
