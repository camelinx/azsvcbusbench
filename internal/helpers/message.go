package helpers

import (
    "fmt"
    "time"
    "encoding/json"
    "math/rand"
)

type MsgType int

const (
    MsgTypeMin      MsgType     = iota
    MsgTypeJson
    MsgTypeMax
)

type msgTypeGenerator func( *Msg )( [ ]byte, error )

var msgTypeGenerators = [ ]msgTypeGenerator {
    MsgTypeJson :   jsonMsgTypeGenerator,
}

type msgTypeParser func( [ ]byte )( *Msg, error )

var msgTypeParsers = [ ]msgTypeParser {
    MsgTypeJson :   jsonMsgTypeParser,
}

type MsgCtx struct {
    ips       [ ]string
    ipsCount     int
    ipClass      Ipv4AddrClass
    msgType      MsgType
}

type Msg struct {
    Current     int     `json:"current"`
    Delta       int     `json:"delta"`
    TimeStamp   int64   `json:"ts"`
    ClientIp    string  `json:"clientip"`
}

func getCurTimeStamp( )( int64 ) {
    return time.Now( ).Unix( )
}

func getRandomInt( n int )( r int ) {
    if 0 == n || 1 == n {
        return n
    }

    r = rand.Intn( n )
    if 0 == r {
        r++
        r += rand.Intn( n - 1 )
    }

    return r
}

func getCounters( )( int, int ) {
    return getRandomInt( 4096 ), getRandomInt( 64 )
}

func InitMsgs( ipCount int, ipClass Ipv4AddrClass, msgType MsgType )( msgCtx *MsgCtx, err error ) {
    if 0 >= ipCount {
        return nil, fmt.Errorf( "ip address count is invalid" )
    }

    if msgType <= MsgTypeMin || msgType >= MsgTypeMax {
        return nil, fmt.Errorf( "invalid message type" )
    }

    msgCtx = &MsgCtx {
        ipClass  :  ipClass,
        msgType  :  msgType,
        ipsCount :  ipCount,
    }

    msgCtx.ips, err = GetIpv4Block( msgCtx.ipsCount, msgCtx.ipClass )
    if err != nil {
        return nil, err
    }

    return msgCtx, nil
}

func ( msgCtx *MsgCtx )GetMsg( )( msg [ ]byte, err error ) {
    current, delta := getCounters( )

    msgInst := &Msg {
        Current     :   current,
        Delta       :   delta,
        TimeStamp   :   getCurTimeStamp( ),
    }

    msgInst.ClientIp = msgCtx.ips[ rand.Intn( msgCtx.ipsCount ) ]

    if msgCtx.msgType > MsgTypeMin && msgCtx.msgType < MsgTypeMax {
        return msgTypeGenerators[ msgCtx.msgType ]( msgInst )
    }

    return nil, fmt.Errorf( "failed to generate message" )
}

func ( msgCtx *MsgCtx )ParseMsg( msg [ ]byte )( msgInst *Msg, err error ) {
    if msgCtx.msgType > MsgTypeMin && msgCtx.msgType < MsgTypeMax {
        return msgTypeParsers[ msgCtx.msgType ]( msg )
    }

    return nil, fmt.Errorf( "failed to parse message" )
}

func jsonMsgTypeGenerator( msgInst *Msg )( msg [ ]byte, err error ) {
    if nil == msgInst {
        return nil, fmt.Errorf( "message not set" )
    }

    return json.Marshal( msgInst )
}

func jsonMsgTypeParser( msg [ ]byte )( msgInst *Msg, err error ) {
    if nil == msg || len( msg ) == 0 {
        return nil, fmt.Errorf( "invalid or empty message" )
    }

    msgInst = &Msg{ }
    err = json.Unmarshal( msg, msgInst )
    return msgInst, err
}
