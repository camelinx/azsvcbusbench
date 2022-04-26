package helpers

import (
    "testing"
    "fmt"
    "strings"
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
        ipStr += "ipv4ReaderBase" + fmt.Sprint( i ) + "\n"
    }

    strReader := strings.NewReader( ipStr )

    return InitMsgGen( strReader, 0, Ipv4AddrClassAny, MsgTypeJson )
}

func ( msgGen *MsgGen )test( t *testing.T ) {
    for i := 0; i < ipv4GenMagicNum; i++ {
        msg, err := msgGen.GetMsg( )
        if err != nil {
            t.Fatalf( "GetMsg - failed to get message" )
        }

        _, err = msgGen.ParseMsg( msg )
        if err != nil {
            t.Fatalf( "GetMsg - invalid message %v", string( msg ) )
        }
    }

    msg, err := msgGen.GetMsg( )
    if err != nil {
        t.Fatalf( "GetMsg - failed to get message" )
    }

    _, err = msgGen.ParseMsg( msg )
    if err != nil {
        t.Fatalf( "GetMsg - invalid message %v", string( msg ) )
    }

    msgGen.msgType = MsgTypeMin
    _, err = msgGen.GetMsg( )
    if err == nil {
        t.Fatalf( "GetMsg - succeeded for invalid message type lower bound" )
    }

    _, err = msgGen.ParseMsg( msg )
    if err == nil {
        t.Fatalf( "ParseMsg - succeeded for invalid message type lower bound" )
    }

    msgGen.msgType = MsgTypeMax
    _, err = msgGen.GetMsg( )
    if err == nil {
        t.Fatalf( "GetMsg - succeeded for invalid message type upper bound" )
    }

    _, err = msgGen.ParseMsg( msg )
    if err == nil {
        t.Fatalf( "ParseMsg - succeeded for invalid message type upper bound" )
    }
}

func TestGetMsg( t *testing.T ) {
    msgGen, err := testInitMsgFromCount( )
    if err != nil {
        t.Fatalf( "InitMsgGen - failed to initialize message context from count" )
    }

    msgGen.test( t )

    msgGen, err = testInitMsgFromReader( )
    if err != nil {
        t.Fatalf( "InitMsgGen - failed to initialize message context from reader" )
    }

    msgGen.test( t )
}
