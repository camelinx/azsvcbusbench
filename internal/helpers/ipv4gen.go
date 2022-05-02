package helpers

import (
    "fmt"
    "io"
    "net"
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

type Ipv4Gen struct {
    Block        [ ]string
    Count           int
    Class           Ipv4AddrClass
    Initialized     bool
}

func NewIpv4Generator( )( *Ipv4Gen ) {
    return &Ipv4Gen{ }
}

func ( ipv4Gen *Ipv4Gen )InitIpv4BlockFromReader( file io.Reader )( err error ) {
    if ipv4Gen.Initialized {
        return nil
    }

    cb := func( ipv4Addr string )( error ) {
        ipv4Gen.Block = append( ipv4Gen.Block, ipv4Addr )
        ipv4Gen.Count++
        return nil
    }

    ipv4Gen.Class = Ipv4AddrClassAny

    err = ProcessFile( file, cb )
    if err != nil {
        return err
    }

    ipv4Gen.Initialized = true
    return nil
}

func ( ipv4Gen *Ipv4Gen )InitIpv4Block( blockCount int, addrClass Ipv4AddrClass )( err error ) {
    if addrClass <= Ipv4AddrClassMin || addrClass >= Ipv4AddrClassMax {
        return fmt.Errorf( "invalid address type %v\n", addrClass )
    }

    if ipv4Gen.Initialized {
        return nil
    }

    ipv4Gen.Class = addrClass
    ipv4Gen.Count = blockCount
    ipv4Gen.Block = make( [ ]string, ipv4Gen.Count )

    ipv4AddrGeneratorHandler := ipv4AddrGenerators[ addrClass ]
    for i := 0; i < blockCount; i++ {
        ipv4Addr, err := ipv4AddrGeneratorHandler( )
        if err != nil {
            return err
        }

        ipv4Gen.Block[ i ] = ipv4Addr
    }

    ipv4Gen.Initialized = true
    return nil
}

func ( ipv4Gen *Ipv4Gen )GetRandomIp( )( ipv4Addr string, err error ) {
    if ipv4Gen.Initialized {
        return ipv4Gen.Block[ rand.Intn( ipv4Gen.Count ) ], nil
    }

    return "", fmt.Errorf( "generator not initialized" )
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

type ipv4Validator func( string )( error )

var ipv4Validators = [ ]ipv4Validator {
    Ipv4AddrClassAny        :   validateClassAny,
    Ipv4AddrClassA          :   validateClassA,
    Ipv4AddrClassAPrivate   :   validateClassAPrivate,
    Ipv4AddrClassLoopback   :   validateClassLoopback,
}

func ( ipv4Gen *Ipv4Gen )ValidateIpv4Address( ipv4Addr string )( err error ) {
    if ipv4Gen.Class <= Ipv4AddrClassMin || ipv4Gen.Class >= Ipv4AddrClassMax {
        return fmt.Errorf( "invalid address class %v", ipv4Gen.Class )
    }

    return ipv4Validators[ ipv4Gen.Class ]( ipv4Addr )
}

func validateClassAny( ipv4Addr string )( err error ) {
    nip := net.ParseIP( ipv4Addr ).To4( )
    if nil == nip {
        return fmt.Errorf( "invalid ip address %v", ipv4Addr )
    }

    if nip[ 0 ] <= ipv4MinOctet {
        return fmt.Errorf( "invalid ip address %v starting with octet 0", ipv4Addr )
    }

    return nil
}

func validateClassA( ipv4Addr string )( err error ) {
    nip := net.ParseIP( ipv4Addr ).To4( )
    if nil == nip {
        return fmt.Errorf( "invalid ip address %v", ipv4Addr )
    }

    if nip[ 0 ] <= ipv4MinOctet || nip[ 0 ] > ipv4ClassAMaxOctet {
        return fmt.Errorf( "not a class A ip address %v", ipv4Addr )
    }

    if nip[ 0 ] == ipv4ClassAPrivateFirstOctet {
        return fmt.Errorf( "class A private ip address %v", ipv4Addr )
    }

    return nil
}

func validateClassAPrivate( ipv4Addr string )( err error ) {
    nip := net.ParseIP( ipv4Addr ).To4( )
    if nil == nip {
        return fmt.Errorf( "invalid ip address %v", ipv4Addr )
    }

    if nip[ 0 ] != ipv4ClassAPrivateFirstOctet {
        return fmt.Errorf( "not a class A private ip address %v", ipv4Addr )
    }

    return nil
}

func validateClassLoopback( ipv4Addr string )( err error ) {
    nip := net.ParseIP( ipv4Addr ).To4( )
    if nil == nip {
        return fmt.Errorf( "invalid ip address %v", ipv4Addr )
    }

    if nip[ 0 ] != ipv4LoopbackFirstOctet {
        return fmt.Errorf( "not a loopback ip address %v", ipv4Addr )
    }

    return nil
}
