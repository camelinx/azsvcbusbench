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
    RandomCur   =   4096
    RandomDelta =   64
)

const (
    MsgTypeMin      MsgType     = iota
    MsgTypeJson
    MsgTypeMax
)

type msgTypeGenerator func( *Msgs )( [ ]byte, error )

var msgTypeGenerators = [ ]msgTypeGenerator {
    MsgTypeJson :   jsonMsgTypeGenerator,
}

type msgTypeParser func( [ ]byte )( *Msgs, error )

var msgTypeParsers = [ ]msgTypeParser {
    MsgTypeJson :   jsonMsgTypeParser,
}

type MsgGen struct {
    ipv4Gen     *Ipv4Gen
    msgType      MsgType
}

type Msg struct {
    Current     int                         `json:"current"`
    Delta       int                         `json:"delta"`
    ClientIp    string                      `json:"clientip"`
    Attributes  map[ string ]interface{ }   `json:"attrs,omitempty"`
}

type Msgs struct {
    List     [ ]Msg                         `json:"messages"`
    Count       int                         `json:"count"`
    TimeStamp   int64                       `json:"ts"`
}

func GetCurTimeStamp( )( int64 ) {
    return time.Now( ).UnixMilli( )
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
    return getRandomInt( RandomCur ), getRandomInt( RandomDelta )
}

func ( msg *Msg )validateCounters( )( err error ) {
    if msg.Current < 0 || msg.Current >= RandomCur {
        return fmt.Errorf( "invalid current counter %v", msg.Current )
    }

    if msg.Delta < 0 || msg.Delta >= RandomDelta {
        return fmt.Errorf( "invalid delta counter %v", msg.Delta )
    }

    return nil
}

func InitMsgGen( file io.Reader, ipCount int, ipClass Ipv4AddrClass, msgType MsgType )( msgGen *MsgGen, err error ) {
    if ipCount <= 0 && file == nil {
        return nil, fmt.Errorf( "ip address count and reader are invalid" )
    }

    if msgType <= MsgTypeMin || msgType >= MsgTypeMax {
        return nil, fmt.Errorf( "invalid message type" )
    }

    msgGen = &MsgGen {
        msgType  :  msgType,
    }

    msgGen.ipv4Gen = NewIpv4Generator( )

    if file != nil {
        err = msgGen.ipv4Gen.InitIpv4BlockFromReader( file )
    } else {
        err = msgGen.ipv4Gen.InitIpv4Block( ipCount, ipClass )
    }

    if err != nil {
        return nil, err
    }

    return msgGen, nil
}

func ( msgGen *MsgGen )getMsgInst( attributes map[ string ]interface{ } )( msgInst *Msg, err error ) {
    current, delta := getCounters( )

    msgInst = &Msg {
        Current     :   current,
        Delta       :   delta,
        Attributes  :   attributes,
    }

    msgInst.ClientIp, err = msgGen.ipv4Gen.GetRandomIp( )
    if err != nil {
        return nil, err
    }

    return msgInst, nil
}

func ( msgGen *MsgGen )getMsgNInternal( n int, attributes map[ string ]interface{ } )( msg [ ]byte, msgList *Msgs, err error ) {
    msgList = &Msgs {
        Count       :   n,
        List        :   make( [ ]Msg, n ),
        TimeStamp   :   GetCurTimeStamp( ),
    }

    for i := 0; i < n; i++ {
        msgInst, err := msgGen.getMsgInst( attributes )
        if err != nil {
            return nil, nil, err
        }

        msgList.List[ i ] = *msgInst
    }

    if msgGen.msgType > MsgTypeMin && msgGen.msgType < MsgTypeMax {
        msg, err = msgTypeGenerators[ msgGen.msgType ]( msgList )
        if err != nil {
            return nil, nil, err
        }

        return msg, msgList, nil
    }

    return nil, nil, fmt.Errorf( "failed to generate message" )
}

func ( msgGen *MsgGen )GetRandomMsgKey( )( key string, err error ) {
    return msgGen.ipv4Gen.GetRandomIp( )
}

func ( msgGen *MsgGen )GetMsgN( n int, attributes map[ string ]interface{ } )( msg [ ]byte, err error ) {
    msg, _, err = msgGen.getMsgNInternal( n, attributes )
    if err != nil {
        return nil, err
    }

    return msg, nil
}

func ( msgGen *MsgGen )GetMsgNWithKeys( n int, attributes map[ string ]interface{ } )( msg [ ]byte, keys [ ]string, err error ) {
    msg, msgList, err := msgGen.getMsgNInternal( n, attributes )
    if err != nil {
        return nil, nil, err
    }

    keys = make( [ ]string, msgList.Count )
    for i, msgInst := range msgList.List {
        keys[ i ] = msgInst.ClientIp
    }

    return msg, keys, nil
}

func ( msgGen *MsgGen )GetMsg( attributes map[ string ]interface{ } )( msg [ ]byte, err error ) {
    return msgGen.GetMsgN( 1, attributes )
}

func ( msgGen *MsgGen )GetMsgWithKey( attributes map[ string ]interface{ } )( msg [ ]byte, key string, err error ) {
    msg, keys, err := msgGen.GetMsgNWithKeys( 1, attributes )
    if err != nil {
        return nil, "", err
    }

    return msg, keys[ 0 ], nil
}

type MsgCb func( *Msg )( error )

func ( msgGen *MsgGen )ParseMsg( msg [ ]byte, cb MsgCb )( msgList *Msgs, err error ) {
    if msgGen.msgType > MsgTypeMin && msgGen.msgType < MsgTypeMax {
        msgList, err = msgTypeParsers[ msgGen.msgType ]( msg )
        if err != nil {
            return nil, err
        }

        if cb != nil {
            for _, msg := range msgList.List {
                err = cb( &msg )
                if err != nil {
                    return nil, err
                }
            }
        }

        return msgList, nil
    }

    return nil, fmt.Errorf( "failed to parse message" )
}

func ( msgGen *MsgGen )ValidateMsg( msg *Msg )( err error ) {
    err = msg.validateCounters( )
    if err != nil {
        return err
    }

    return msgGen.ipv4Gen.ValidateIpv4Address( msg.ClientIp )
}

func ( msgList *Msgs )GetLatency( )( latency int64 ) {
    curTimeStamp := GetCurTimeStamp( )
    if msgList.TimeStamp <= curTimeStamp {
        return curTimeStamp - msgList.TimeStamp
    }

    return 0
}

func jsonMsgTypeGenerator( msgList *Msgs )( msg [ ]byte, err error ) {
    if nil == msgList {
        return nil, fmt.Errorf( "message not set" )
    }

    return json.Marshal( msgList )
}

func jsonMsgTypeParser( msg [ ]byte )( msgList *Msgs, err error ) {
    if nil == msg || len( msg ) == 0 {
        return nil, fmt.Errorf( "invalid or empty message" )
    }

    msgList = &Msgs{ }
    err = json.Unmarshal( msg, msgList )
    return msgList, err
}
