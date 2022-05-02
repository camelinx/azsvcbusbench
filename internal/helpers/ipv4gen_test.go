package helpers

import (
    "testing"
    "fmt"
    "strings"
)

const (
    ipv4GenMagicNum = 32
    ipv4ReaderBase  = "10.0.0."
    ipv4ReaderStr   = "10.0.0.1\n10.0.0.2\n10.0.0.3"
)

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
            err = ipv4Gen.ValidateIpv4Address( ipv4Gen.Block[ j ] )
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

            err = ipv4Gen.ValidateIpv4Address( randomIp )
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

        err = ipv4Gen.ValidateIpv4Address( randomIp )
        if err != nil {
            t.Fatalf( "GetRandomIp - invalid ip address %v from reader: error %v", randomIp, err )
        }
    }
}

type negativeTester func( *testing.T, *Ipv4Gen  )

var negativeTesters = [ ]negativeTester {
    Ipv4AddrClassAny        :   negativeTestClassAny,
    Ipv4AddrClassA          :   negativeTestClassA,
    Ipv4AddrClassAPrivate   :   negativeTestClassAPrivate,
    Ipv4AddrClassLoopback   :   negativeTestClassLoopback,
}

func negativeTestClassAny( t *testing.T, ipv4Gen *Ipv4Gen ) {
    invalidIps := [ ]string { "0.1.2.3", "11.12.13.256", "121.256.23.24", "131.32.256.34", "256.242.43.44", "256.256.256.256" }

    for _, invalidIp := range invalidIps {
        err := ipv4Gen.ValidateIpv4Address( invalidIp )
        if err == nil {
            t.Fatalf( "ValidateIpv4Address - failed to detect invalid ip address %v for class any", invalidIp )
        }
    }
}

func negativeTestClassA( t *testing.T, ipv4Gen *Ipv4Gen ) {
    invalidIps := [ ]string { "0.1.2.3", "11.12.13.256", "21.256.23.24", "31.32.256.34", "256.42.43.44", "127.0.0.1", "10.0.0.1", "128.0.0.1", "192.0.0.1", "172.0.0.1" }

    for _, invalidIp := range invalidIps {
        err := ipv4Gen.ValidateIpv4Address( invalidIp )
        if err == nil {
            t.Fatalf( "ValidateIpv4Address - failed to detect invalid ip address %v for class A", invalidIp )
        }
    }
}

func negativeTestClassAPrivate( t *testing.T, ipv4Gen *Ipv4Gen ) {
    invalidIps := [ ]string { "0.1.2.3", "11.12.13.256", "21.256.23.24", "31.32.256.34", "256.42.43.44", "127.0.0.1", "128.0.0.1", "192.0.0.1", "172.0.0.1" }

    for _, invalidIp := range invalidIps {
        err := ipv4Gen.ValidateIpv4Address( invalidIp )
        if err == nil {
            t.Fatalf( "ValidateIpv4Address - failed to detect invalid ip address %v for class A private", invalidIp )
        }
    }
}

func negativeTestClassLoopback( t *testing.T, ipv4Gen *Ipv4Gen ) {
    invalidIps := [ ]string { "0.1.2.3", "11.12.13.256", "21.256.23.24", "31.32.256.34", "256.42.43.44", "10.0.0.1", "128.0.0.1", "192.0.0.1", "172.0.0.1" }

    for _, invalidIp := range invalidIps {
        err := ipv4Gen.ValidateIpv4Address( invalidIp )
        if err == nil {
            t.Fatalf( "ValidateIpv4Address - failed to detect invalid ip address %v for loopback", invalidIp )
        }
    }
}

func TestValidateIpv4Address( t *testing.T ) {
    for class := Ipv4AddrClassAny; class <= Ipv4AddrClassLoopback; class++ {
        ipv4Gen := testNewIpv4Generator( t )
        err     := ipv4Gen.InitIpv4Block( ipv4GenMagicNum, class )
        if err != nil || !ipv4Gen.Initialized {
            t.Fatalf( "InitIpv4Block - failed to initialize from count" )
        }

        for j := 0; j < ipv4GenMagicNum; j++ {
            err = ipv4Gen.ValidateIpv4Address( ipv4Gen.Block[ j ] )
            if err != nil {
                t.Fatalf( "ValidateIpv4Address - invalid ip address %v for count %v and class %v: error %v", ipv4Gen.Block[ j ], ipv4GenMagicNum, class, err )
            }
        }

        negativeTesters[ class ]( t, ipv4Gen )
    }
}
