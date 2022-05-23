package helpers

import (
    "testing"
    "fmt"
    "strings"
)

const (
    msgConst = 10
)

func TestInitMsgGen( t *testing.T ) {
    _, err := InitMsgGen( nil, getRandomInt( ipv4GenMagicNum ), Ipv4AddrClassAny, MsgTypeJson )
    if err != nil {
        t.Fatalf( "InitMsgGen - failed to initialize message context" )
    }

    strReader := strings.NewReader( ipv4ReaderStr )
    _, err = InitMsgGen( strReader, 0, Ipv4AddrClassAny, MsgTypeJson )
    if err != nil {
        t.Fatalf( "InitMsgGen - failed to initialize message context from reader" )
    }

    _, err = InitMsgGen( nil, getRandomInt( ipv4GenMagicNum ), Ipv4AddrClassAny, MsgTypeMin )
    if err == nil {
        t.Fatalf( "InitMsgGen - successfully initialized for invalid message type lower bound" )
    }

    _, err = InitMsgGen( nil, getRandomInt( ipv4GenMagicNum ), Ipv4AddrClassAny, MsgTypeMax )
    if err == nil {
        t.Fatalf( "InitMsgGen - successfully initialized for invalid message type upper bound" )
    }

    _, err = InitMsgGen( nil, 0, Ipv4AddrClassAny, MsgTypeJson )
    if err == nil {
        t.Fatalf( "InitMsgGen - successfully initialized for 0 ip count and nil reader" )
    }

    _, err = InitMsgGen( nil, -1, Ipv4AddrClassAny, MsgTypeJson )
    if err == nil {
        t.Fatalf( "InitMsgGen - successfully initialized for negative ip count" )
    }

    _, err = InitMsgGen( nil, getRandomInt( ipv4GenMagicNum ), Ipv4AddrClassMin, MsgTypeJson )
    if err == nil {
        t.Fatalf( "InitMsgGen - successfully initialized for invalid ip address class lower bound" )
    }

    _, err = InitMsgGen( nil, getRandomInt( ipv4GenMagicNum ), Ipv4AddrClassMax, MsgTypeJson )
    if err == nil {
        t.Fatalf( "InitMsgGen - successfully initialized for invalid ip address class upper bound" )
    }

    for class := Ipv4AddrClassMin + 1; class < Ipv4AddrClassMax; class++ {
        _, err = InitMsgGen( nil, getRandomInt( ipv4GenMagicNum ), class, MsgTypeJson )
        if err != nil {
            t.Fatalf( "InitMsgGen - failed to initialize for valid ip address class %v", class )
        }

        strReader = strings.NewReader( ipv4ReaderStr )
        _, err = InitMsgGen( strReader, 0, class, MsgTypeJson )
        if err != nil {
            t.Fatalf( "InitMsgGen - failed to initialize from reader for valid ip address class %v", class )
        }
    }
}

func testInitMsgFromCount( )( msgGen *MsgGen, err error ) {
    return InitMsgGen( nil, getRandomInt( ipv4GenMagicNum ), Ipv4AddrClassAny, MsgTypeJson )
}

func testInitMsgFromReader( )( msgGen *MsgGen, err error ) {
    var ipStr string

    for i := 1; i <= ipv4GenMagicNum; i++ {
        ipStr += ipv4ReaderBase + fmt.Sprint( i ) + "\n"
    }

    strReader := strings.NewReader( ipStr )

    return InitMsgGen( strReader, 0, Ipv4AddrClassAny, MsgTypeJson )
}

func ( msgGen *MsgGen )test( t *testing.T, attributes map[ string ]interface{ } ) {
    msgCallback := func( msg *Msg )( err error ) {
        return msgGen.ValidateMsg( msg )
    }

    for i := 0; i < ipv4GenMagicNum; i++ {
        msg, err := msgGen.GetMsg( attributes )
        if err != nil {
            t.Fatalf( "GetMsg - failed to get message" )
        }

        _, err = msgGen.ParseMsg( msg, nil )
        if err != nil {
            t.Fatalf( "ParseMsg - invalid message %v", string( msg ) )
        }

        msg, err = msgGen.GetMsgN( msgConst, attributes ) 
        if err != nil {
            t.Fatalf( "GetMsgN - failed to get message" )
        }

        _, err = msgGen.ParseMsg( msg, msgCallback )
        if err != nil {
            t.Fatalf( "ParseMsg - invalid message %v", string( msg ) )
        }
    }

    msg, err := msgGen.GetMsg( attributes )
    if err != nil {
        t.Fatalf( "GetMsg - failed to get message" )
    }

    _, err = msgGen.ParseMsg( msg, msgCallback )
    if err != nil {
        t.Fatalf( "ParseMsg - invalid message %v", string( msg ) )
    }

    msg, err = msgGen.GetMsgN( msgConst, attributes )
    if err != nil {
        t.Fatalf( "GetMsgN - failed to get message" )
    }

    _, err = msgGen.ParseMsg( msg, msgCallback )
    if err != nil {
        t.Fatalf( "ParseMsg - invalid message %v", string( msg ) )
    }

    msgGen.msgType = MsgTypeMin
    _, err = msgGen.GetMsg( attributes )
    if err == nil {
        t.Fatalf( "GetMsg - succeeded for invalid message type lower bound" )
    }

    _, err = msgGen.ParseMsg( msg, msgCallback )
    if err == nil {
        t.Fatalf( "ParseMsg - succeeded for invalid message type lower bound" )
    }

    _, err = msgGen.GetMsgN( msgConst, attributes )
    if err == nil {
        t.Fatalf( "GetMsgN - succeeded for invalid message type lower bound" )
    }

    _, err = msgGen.ParseMsg( msg, msgCallback )
    if err == nil {
        t.Fatalf( "ParseMsg - succeeded for invalid message type lower bound" )
    }

    msgGen.msgType = MsgTypeMax
    _, err = msgGen.GetMsg( attributes )
    if err == nil {
        t.Fatalf( "GetMsg - succeeded for invalid message type upper bound" )
    }

    _, err = msgGen.ParseMsg( msg, msgCallback )
    if err == nil {
        t.Fatalf( "ParseMsg - succeeded for invalid message type upper bound" )
    }

    _, err = msgGen.GetMsgN( msgConst, attributes )
    if err == nil {
        t.Fatalf( "GetMsgN - succeeded for invalid message type lower bound" )
    }

    _, err = msgGen.ParseMsg( msg, msgCallback )
    if err == nil {
        t.Fatalf( "ParseMsg - succeeded for invalid message type lower bound" )
    }
}

func TestGetMsg( t *testing.T ) {
    msgGen, err := testInitMsgFromCount( )
    if err != nil {
        t.Fatalf( "InitMsgGen - failed to initialize message context from count" )
    }

    msgGen.test( t, nil )

    msgGen, err = testInitMsgFromCount( )
    if err != nil {
        t.Fatalf( "InitMsgGen - failed to initialize message context from count" )
    }

    msgGen.test( t, map[ string ]interface{ } {
        "key1"  :   "value1",
        "key2"  :   2,
        "key3"  :   9.8,
    } )

    msgGen, err = testInitMsgFromReader( )
    if err != nil {
        t.Fatalf( "InitMsgGen - failed to initialize message context from reader" )
    }

    msgGen.test( t, nil )

    msgGen, err = testInitMsgFromReader( )
    if err != nil {
        t.Fatalf( "InitMsgGen - failed to initialize message context from count" )
    }

    msgGen.test( t, map[ string ]interface{ } {
        "key1"  :   "value1",
        "key2"  :   42,
        "key3"  :   3.14,
    } )
}
