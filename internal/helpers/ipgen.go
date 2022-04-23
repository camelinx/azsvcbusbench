package helpers

import (
    "fmt"
    "math/rand"
)

const (
    ipv4MinOctet                  =   0
    ipv4MaxOctet                  =   255
    ipv4ClassAMaxOctet            =   126
    ipv4ClassAPrivateFirstOctet   =   10
    ipv4LoopbackFirstOctet        =   127
)

type Ipv4AddrClass int

const (
    Ipv4AddrClassMin Ipv4AddrClass = iota
    Ipv4AddrClassAny
    Ipv4AddrClassA
    Ipv4AddrClassAPrivate
    Ipv4AddrClassLoopback
    Ipv4AddrClassMax
)

type ipv4AddrGenerator func( )( string, error )

var ipv4AddrGenerators = [ ]ipv4AddrGenerator {
    Ipv4AddrClassAny         :    getAnyIpv4,
    Ipv4AddrClassA           :    getClassAIpv4,
    Ipv4AddrClassAPrivate    :    getClassAPrivateIpv4,
    Ipv4AddrClassLoopback    :    getLoopbackIpv4,
}

func GetIpv4Block( blockCount int, addrClass Ipv4AddrClass )( block [ ]string, err error ) {
    if addrClass <= Ipv4AddrClassMin || addrClass >= Ipv4AddrClassMax {
        return nil, fmt.Errorf( "invalid address type %v\n", addrClass )
    }

    ipv4AddrGeneratorHandler := ipv4AddrGenerators[ addrClass ]
    for i := 0; i < blockCount; i++ {
        ipv4Addr, _ := ipv4AddrGeneratorHandler( )
        block = append( block, ipv4Addr )
    }

    return block, nil
}

func getAnyIpv4( )( string, error ) {
    octets := make( [ ]int, 4 )

    octets[ 0 ], _ = genIpv4OctetWithExcludeList(
        ipv4MinOctet,
        ipv4MaxOctet,
        [ ]int{ ipv4MinOctet },
    )

    for oi := 1; oi < 4; oi++ {
        octets[ oi ], _ = genIpv4Octet( ipv4MinOctet, ipv4MaxOctet )
    }

    return getIpv4StringFromOctets( octets ), nil
}

func getClassAIpv4( )( string, error ) {
    octets := make( [ ]int, 4 )

    octets[ 0 ], _ = genIpv4OctetWithExcludeList(
        ipv4MinOctet,
        ipv4ClassAMaxOctet,
        [ ]int{ ipv4MinOctet, ipv4ClassAPrivateFirstOctet },
    )

    for oi := 1; oi < 4; oi++ {
        octets[ oi ], _ = genIpv4Octet( ipv4MinOctet, ipv4MaxOctet )
    }

    return getIpv4StringFromOctets( octets ), nil
}

func getClassAPrivateIpv4( )( string, error ) {
    octets := make( [ ]int, 4 )

    octets[ 0 ] = ipv4ClassAPrivateFirstOctet

    for oi := 1; oi < 4; oi++ {
        octets[ oi ], _ = genIpv4Octet( ipv4MinOctet, ipv4MaxOctet )
    }

    return getIpv4StringFromOctets( octets ), nil
}

func getLoopbackIpv4( )( string, error ) {
    octets := make( [ ]int, 4 )

    octets[ 0 ] = ipv4LoopbackFirstOctet

    for oi := 1; oi < 4; oi++ {
        octets[ oi ], _ = genIpv4Octet( ipv4MinOctet, ipv4MaxOctet )
    }

    return getIpv4StringFromOctets( octets ), nil
}

func genIpv4Octet( min, max int )( int, error ) {
    return genIpv4OctetWithExcludeList( min, max, [ ]int{ } )
}

func genIpv4OctetWithExcludeList( min, max int, excludeList [ ]int )( int, error ) {
    if max < 0 {
        return 0, fmt.Errorf( "invalid max: cannot be negative" )
    }

    excludeMap := make( map[ int ]bool )
    for _, exclude := range excludeList {
        excludeMap[ exclude ] = true
    }

    octet := rand.Intn( max + 1 )
    if octet < min {
        octet += rand.Intn( max - min )
    }

    if _, exists := excludeMap[ octet ]; exists {
        for octet = min; octet <= max; octet++ {
            if _, exists := excludeMap[ octet ]; !exists {
                break
            }
        }
    }

    return octet, nil
}

func getIpv4StringFromOctets( octets [ ]int )( ipv4Addr string ) {
    return fmt.Sprintf( "%d.%d.%d.%d", octets[ 0 ], octets[ 1 ], octets[ 2 ], octets[ 3 ] )
}
