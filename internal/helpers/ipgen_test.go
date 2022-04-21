package helpers

import (
    "testing"
    "net"
    "math/rand"
    "fmt"
)

func getBlockCount( )( int ) {
    return rand.Intn( 32 )
}

type ipv4Validator func( string )( error )

var ipv4Validators = [ ]ipv4Validator {
    Ipv4AddrClassAny        :   validateClassAny,
    Ipv4AddrClassA          :   validateClassA,
    Ipv4AddrClassAPrivate   :   validateClassAPrivate,
    Ipv4AddrClassLoopback   :   validateClassLoopback,
}

func validateClassAny( sip string )( err error ) {
    if nil == net.ParseIP( sip ).To4( ) {
        return fmt.Errorf( "invalid ip address" )
    }

    return nil
}

func validateClassA( sip string )( err error ) {
    nip := net.ParseIP( sip ).To4( )
    if nil == nip {
        return fmt.Errorf( "invalid ip address" )
    }

    if nip[ 0 ] <= ipv4MinOctet || nip[ 0 ] > ipv4ClassAMaxOctet {
        return fmt.Errorf( "not a class A ip address" )
    }

    if nip[ 0 ] == ipv4ClassAPrivateFirstOctet {
        return fmt.Errorf( "class A private ip address" )
    }

    return nil
}

func validateClassAPrivate( sip string )( err error ) {
    nip := net.ParseIP( sip ).To4( )
    if nil == nip {
        return fmt.Errorf( "invalid ip address" )
    }

    if nip[ 0 ] != ipv4ClassAPrivateFirstOctet {
        return fmt.Errorf( "not a class A private ip address" )
    }

    return nil
}

func validateClassLoopback( sip string )( err error ) {
    nip := net.ParseIP( sip ).To4( )
    if nil == nip {
        return fmt.Errorf( "invalid ip address" )
    }

    if nip[ 0 ] != ipv4LoopbackFirstOctet {
        return fmt.Errorf( "not a loopback ip address" )
    }

    return nil
}

func testIpv4Block( t *testing.T, count int, class Ipv4AddrType ) {
    block, err := GetIpv4Block( count, class )
    if err != nil {
        t.Errorf( "GetIpv4Block - error for count %v and class %v: error %v", count, class, err )
    }

    for _, ip := range block {
        err = ipv4Validators[ class ]( ip )
        if err != nil {
            t.Errorf( "GetIpv4Block - invalid ip address %v for count %v and class %v: error %v", ip, count, class, err )
        }
    }
}

func TestGetIpv4Block( t *testing.T ) {
    testIpv4Block( t, getBlockCount( ), Ipv4AddrClassAny )
    testIpv4Block( t, getBlockCount( ), Ipv4AddrClassA )
    testIpv4Block( t, getBlockCount( ), Ipv4AddrClassAPrivate )
    testIpv4Block( t, getBlockCount( ), Ipv4AddrClassLoopback )
}
