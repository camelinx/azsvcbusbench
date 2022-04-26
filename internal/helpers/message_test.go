package helpers

import (
    "testing"
    "fmt"
    "strings"
)

func TestInitMsgs( t *testing.T ) {
    _, err := InitMsgs( nil, getRandomInt( 32 ), Ipv4AddrClassAny, MsgTypeJson )
    if err != nil {
        t.Errorf( "InitMsgs - failed to initialize message context" )
    }

    strReader := strings.NewReader( "10.0.0.1\n10.0.0.2\n10.0.0.3" )
    _, err = InitMsgs( strReader, 0, Ipv4AddrClassAny, MsgTypeJson )
    if err != nil {
        t.Errorf( "InitMsgs - failed to initialize message context from reader" )
    }

    _, err = InitMsgs( nil, getRandomInt( 32 ), Ipv4AddrClassAny, MsgTypeMin )
    if err == nil {
        t.Errorf( "InitMsgs - successfully initialized for invalid message type lower bound" )
    }

    _, err = InitMsgs( nil, getRandomInt( 32 ), Ipv4AddrClassAny, MsgTypeMax )
    if err == nil {
        t.Errorf( "InitMsgs - successfully initialized for invalid message type upper bound" )
    }

    _, err = InitMsgs( nil, 0, Ipv4AddrClassAny, MsgTypeJson )
    if err == nil {
        t.Errorf( "InitMsgs - successfully initialized for 0 ip count and nil reader" )
    }

    _, err = InitMsgs( nil, -1, Ipv4AddrClassAny, MsgTypeJson )
    if err == nil {
        t.Errorf( "InitMsgs - successfully initialized for negative ip count" )
    }

    _, err = InitMsgs( nil, getRandomInt( 32 ), Ipv4AddrClassMin, MsgTypeJson )
    if err == nil {
        t.Errorf( "InitMsgs - successfully initialized for invalid ip address class lower bound" )
    }

    _, err = InitMsgs( nil, getRandomInt( 32 ), Ipv4AddrClassMax, MsgTypeJson )
    if err == nil {
        t.Errorf( "InitMsgs - successfully initialized for invalid ip address class upper bound" )
    }

    for class := Ipv4AddrClassMin + 1; class < Ipv4AddrClassMax; class++ {
        _, err = InitMsgs( nil, getRandomInt( 32 ), class, MsgTypeJson )
        if err != nil {
            t.Errorf( "InitMsgs - failed to initialize for valid ip address class %v", class )
        }

        strReader = strings.NewReader( "10.0.0.1\n10.0.0.2\n10.0.0.3" )
        _, err = InitMsgs( strReader, 0, class, MsgTypeJson )
        if err != nil {
            t.Errorf( "InitMsgs - failed to initialize from reader for valid ip address class %v", class )
        }
    }
}

func testInitMsgFromCount( )( msgs *Msgs, err error ) {
    return InitMsgs( nil, getRandomInt( 32 ), Ipv4AddrClassAny, MsgTypeJson )
}

func testInitMsgFromReader( )( msgs *Msgs, err error ) {
    var ipStr string

    for i := 1; i <= 32; i++ {
        ipStr += "10.0.0." + fmt.Sprint( i ) + "\n"
    }

    strReader := strings.NewReader( ipStr )

    return InitMsgs( strReader, 0, Ipv4AddrClassAny, MsgTypeJson )
}

func ( msgs *Msgs )test( t *testing.T ) {
    for i := 0; i < 32; i++ {
        msg, err := msgs.GetMsg( )
        if err != nil {
            t.Errorf( "GetMsg - failed to get message" )
        }

        _, err = msgs.ParseMsg( msg )
        if err != nil {
            t.Errorf( "GetMsg - invalid message %v", string( msg ) )
        }
    }

    msg, err := msgs.GetMsg( )
    if err != nil {
        t.Errorf( "GetMsg - failed to get message" )
    }

    _, err = msgs.ParseMsg( msg )
    if err != nil {
        t.Errorf( "GetMsg - invalid message %v", string( msg ) )
    }

    msgs.msgType = MsgTypeMin
    _, err = msgs.GetMsg( )
    if err == nil {
        t.Errorf( "GetMsg - succeeded for invalid message type lower bound" )
    }

    _, err = msgs.ParseMsg( msg )
    if err == nil {
        t.Errorf( "ParseMsg - succeeded for invalid message type lower bound" )
    }

    msgs.msgType = MsgTypeMax
    _, err = msgs.GetMsg( )
    if err == nil {
        t.Errorf( "GetMsg - succeeded for invalid message type upper bound" )
    }

    _, err = msgs.ParseMsg( msg )
    if err == nil {
        t.Errorf( "ParseMsg - succeeded for invalid message type upper bound" )
    }
}

func TestGetMsg( t *testing.T ) {
    msgs, err := testInitMsgFromCount( )
    if err != nil {
        t.Errorf( "InitMsgs - failed to initialize message context from count" )
    }

    msgs.test( t )

    msgs, err = testInitMsgFromReader( )
    if err != nil {
        t.Errorf( "InitMsgs - failed to initialize message context from reader" )
    }

    msgs.test( t )
}
