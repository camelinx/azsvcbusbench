package helpers

import (
    "testing"
    "net"
    "fmt"
    "strings"
)

const (
    ipv4GenMagicNum = 32
    ipv4ReaderBase  = "10.0.0."
    ipv4ReaderStr   = "10.0.0.1\n10.0.0.2\n10.0.0.3"
)

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

func testNewIpv4Generator( t *testing.T )( ipv4Gen *Ipv4Gen ) {
    ipv4Gen = NewIpv4Generator( )
    if nil == ipv4Gen {
        t.Fatalf( "NewIpv4Generator - failed to initialize" )
    }

    return ipv4Gen
}

func TestInitIpv4BlockFromReader( t *testing.T ) {
    ipv4Gen   := testNewIpv4Generator( t )
    strReader := strings.NewReader( ipv4ReaderStr )
    err       := ipv4Gen.InitIpv4BlockFromReader( strReader )
    if err != nil || !ipv4Gen.Initialized {
        t.Fatalf( "InitIpv4BlockFromReader - failed to initialize from reader" )
    }

    err = ipv4Gen.InitIpv4Block( ipv4GenMagicNum, Ipv4AddrClassAny )
    if err != nil || !ipv4Gen.Initialized {
        t.Fatalf( "InitIpv4Block - failed to detect earlier initialization from reader" )
    }

    ipv4Gen = testNewIpv4Generator( t )
    err     = ipv4Gen.InitIpv4BlockFromReader( nil )
    if err == nil {
        t.Fatalf( "InitIpv4BlockFromReader - successfully initialized from nil reader" )
    }
}

func TestInitIpv4Block( t *testing.T ) {
    ipv4Gen := testNewIpv4Generator( t )
    err     := ipv4Gen.InitIpv4Block( ipv4GenMagicNum, Ipv4AddrClassAny )
    if err != nil || !ipv4Gen.Initialized {
        t.Fatalf( "InitIpv4Block - failed to initialize from count" )
    }

    strReader := strings.NewReader( ipv4ReaderStr )
    err        = ipv4Gen.InitIpv4BlockFromReader( strReader )
    if err != nil || !ipv4Gen.Initialized {
        t.Fatalf( "InitIpv4BlockFromReader - failed to detect earlier initialization from count" )
    }

    ipv4Gen = testNewIpv4Generator( t )
    err     = ipv4Gen.InitIpv4Block( ipv4GenMagicNum, Ipv4AddrClassMin )
    if err == nil {
        t.Fatalf( "InitIpv4Block - successfully initialized for invalid address class - lower bound" )
    }

    err = ipv4Gen.InitIpv4Block( ipv4GenMagicNum, Ipv4AddrClassMax )
    if err == nil {
        t.Fatalf( "InitIpv4Block - successfully initialized for invalid address class - upper bound" )
    }

    for class := Ipv4AddrClassAny; class <= Ipv4AddrClassLoopback; class++ {
        ipv4Gen  = testNewIpv4Generator( t )
        err      = ipv4Gen.InitIpv4Block( ipv4GenMagicNum, class )
        if err != nil || !ipv4Gen.Initialized {
            t.Fatalf( "InitIpv4Block - failed to initialize from count" )
        }

        for j := 0; j < ipv4GenMagicNum; j++ {
            err = ipv4Validators[ class ]( ipv4Gen.Block[ j ] )
            if err != nil {
                t.Fatalf( "GetIpv4Block - invalid ip address %v for count %v and class %v: error %v", ipv4Gen.Block[ j ], ipv4GenMagicNum, class, err )
            }
        }
    }
}

func TestGetRandomIp( t *testing.T ) {
    for class := Ipv4AddrClassAny; class <= Ipv4AddrClassLoopback; class++ {
        ipv4Gen := testNewIpv4Generator( t )
        err     := ipv4Gen.InitIpv4Block( ipv4GenMagicNum, class )
        if err != nil || !ipv4Gen.Initialized {
            t.Fatalf( "InitIpv4Block - failed to initialize from count" )
        }

        for j := 0; j < ipv4GenMagicNum; j++ {
            randomIp, err := ipv4Gen.GetRandomIp( )
            if err != nil {
                t.Fatalf( "GetRandomIp - error %v", err )
            }

            err = ipv4Validators[ class ]( randomIp )
            if err != nil {
                t.Fatalf( "GetRandomIp - invalid ip address %v for count %v and class %v: error %v", randomIp, ipv4GenMagicNum, class, err )
            }
        }
    }

    var ipStr string

    for i := 1; i <= ipv4GenMagicNum; i++ {
        ipStr += ipv4ReaderBase + fmt.Sprint( i ) + "\n"
    }

    strReader := strings.NewReader( ipStr )
    ipv4Gen   := testNewIpv4Generator( t )
    err       := ipv4Gen.InitIpv4BlockFromReader( strReader )
    if err != nil || !ipv4Gen.Initialized {
        t.Fatalf( "InitIpv4BlockFromReader - failed to initialize from reader" )
    }

    for j := 0; j < ipv4GenMagicNum; j++ {
        randomIp, err := ipv4Gen.GetRandomIp( )
        if err != nil {
            t.Fatalf( "GetRandomIp - error %v", err )
        }

        err = ipv4Validators[ Ipv4AddrClassAPrivate ]( randomIp )
        if err != nil {
            t.Fatalf( "GetRandomIp - invalid ip address %v from reader: error %v", randomIp, err )
        }
    }
}
