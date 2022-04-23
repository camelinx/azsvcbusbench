package helpers

import (
    "testing"
    "encoding/json"
)

func TestInitMsgs( t *testing.T ) {
    _, err := InitMsgs( getRandomInt( 32 ), Ipv4AddrClassAny, MsgTypeJson )
    if err != nil {
        t.Errorf( "InitMsgs - failed to initialize message context" )
    }

    _, err = InitMsgs( getRandomInt( 32 ), Ipv4AddrClassAny, MsgTypeMin )
    if err == nil {
        t.Errorf( "InitMsgs - successfully initialized for invalid message type lower bound" )
    }

    _, err = InitMsgs( getRandomInt( 32 ), Ipv4AddrClassAny, MsgTypeMax )
    if err == nil {
        t.Errorf( "InitMsgs - successfully initialized for invalid message type upper bound" )
    }

    _, err = InitMsgs( 0, Ipv4AddrClassAny, MsgTypeJson )
    if err == nil {
        t.Errorf( "InitMsgs - successfully initialized for 0 ip count" )
    }

    _, err = InitMsgs( -1, Ipv4AddrClassAny, MsgTypeJson )
    if err == nil {
        t.Errorf( "InitMsgs - successfully initialized for negative ip count" )
    }

    _, err = InitMsgs( getRandomInt( 32 ), Ipv4AddrClassMin, MsgTypeJson )
    if err == nil {
        t.Errorf( "InitMsgs - successfully initialized for invalid ip address class lower bound" )
    }

    _, err = InitMsgs( getRandomInt( 32 ), Ipv4AddrClassMax, MsgTypeJson )
    if err == nil {
        t.Errorf( "InitMsgs - successfully initialized for invalid ip address class upper bound" )
    }

    for class := Ipv4AddrClassMin + 1; class < Ipv4AddrClassMax; class++ {
        _, err = InitMsgs( getRandomInt( 32 ), class, MsgTypeJson )
        if err != nil {
            t.Errorf( "InitMsgs - failed to initialize for valid ip address class %v", class )
        }
    }
}

func TestGetMsg( t *testing.T ) {
    msgs, err := InitMsgs( getRandomInt( 32 ), Ipv4AddrClassAny, MsgTypeJson )
    if err != nil {
        t.Errorf( "InitMsgs - failed to initialize message context" )
    }

    var msgInst Msg
    for i := 0; i < 32; i++ {
        msg, err := msgs.GetMsg( )
        if err != nil {
            t.Errorf( "GetMsg - failed to get message" )
        }

        err = json.Unmarshal( msg, &msgInst )
        if err != nil {
            t.Errorf( "GetMsg - invalid message %v", string( msg ) )
        }
    }

    msgs.msgType = MsgTypeMin
    _, err = msgs.GetMsg( )
    if err == nil {
        t.Errorf( "GetMsg - succeeded for invalid message type lower bound" )
    }

    msgs.msgType = MsgTypeMax
    _, err = msgs.GetMsg( )
    if err == nil {
        t.Errorf( "GetMsg - succeeded for invalid message type upper bound" )
    }
}
