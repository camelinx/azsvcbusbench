package helpers

import (
    "math/rand"
)

const alphabets = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func GetRandomString( n uint )( string ) {
    rbytes := make( [ ]byte, n )

    for i, _ := range rbytes { 
        rbytes[ i ] = alphabets[ rand.Intn( len( alphabets ) ) ]
    }

    return string( rbytes )
}

func getJsonKV( kvlen uint )( string, uint ) {
    return "\"" + GetRandomString( kvlen ) + "\":\"" + GetRandomString( kvlen ) + "\"", 1 + kvlen + 3 + kvlen + 1
}

// Does not guarantee a json document exactly of length n
// Final document will be a few bytes longer than n
func GetRandomJsonString( n uint )( string, uint ) {
    kv, kvlen  := getJsonKV( 3 )
    jsonStr    := "{" + kv
    jsonStrLen := 1 +  kvlen

    for jsonStrLen < n {
        kv, kvlen = getJsonKV( 3 )

        jsonStr     += "," + kv
        jsonStrLen  += 1 + kvlen
    }

    jsonStr    += "}"
    jsonStrLen += 1

    return jsonStr, jsonStrLen
}
