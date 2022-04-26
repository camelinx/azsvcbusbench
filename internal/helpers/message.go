package helpers

import (
    "fmt"
    "time"
    "io"
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

type Msgs struct {
    ipv4Gen     *Ipv4Gen
    msgType      MsgType
}

type Msg struct {
    Current     int     `json:"current"`
    Delta       int     `json:"delta"`
    TimeStamp   int64   `json:"ts"`
    ClientIp    string  `json:"clientip"`
}

func GetCurTimeStamp( )( int64 ) {
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

func InitMsgs( file io.Reader, ipCount int, ipClass Ipv4AddrClass, msgType MsgType )( msgs *Msgs, err error ) {
    if ipCount <= 0 && file == nil {
        return nil, fmt.Errorf( "ip address count and reader are invalid" )
    }

    if msgType <= MsgTypeMin || msgType >= MsgTypeMax {
        return nil, fmt.Errorf( "invalid message type" )
    }

    msgs = &Msgs {
        msgType  :  msgType,
    }

    msgs.ipv4Gen = NewIpv4Generator( )

    if file != nil {
        err = msgs.ipv4Gen.InitIpv4BlockFromReader( file )
    } else {
        err = msgs.ipv4Gen.InitIpv4Block( ipCount, ipClass )
    }

    if err != nil {
        return nil, err
    }

    return msgs, nil
}

func ( msgs *Msgs )GetMsg( )( msg [ ]byte, err error ) {
    current, delta := getCounters( )

    msgInst := &Msg {
        Current     :   current,
        Delta       :   delta,
        TimeStamp   :   GetCurTimeStamp( ),
    }

    msgInst.ClientIp, err = msgs.ipv4Gen.GetRandomIp( )
    if err != nil {
        return nil, err
    }

    if msgs.msgType > MsgTypeMin && msgs.msgType < MsgTypeMax {
        return msgTypeGenerators[ msgs.msgType ]( msgInst )
    }

    return nil, fmt.Errorf( "failed to generate message" )
}

func ( msgs *Msgs )ParseMsg( msg [ ]byte )( msgInst *Msg, err error ) {
    if msgs.msgType > MsgTypeMin && msgs.msgType < MsgTypeMax {
        return msgTypeParsers[ msgs.msgType ]( msg )
    }

    return nil, fmt.Errorf( "failed to parse message" )
}

func ( msg *Msg )GetLatency( )( latency int64 ) {
    curTimeStamp := GetCurTimeStamp( )
    if msg.TimeStamp <= curTimeStamp {
        return curTimeStamp - msg.TimeStamp
    }

    return 0
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
