package azredis

import (
    "sync"
    "time"
    "context"
    "os"
    "fmt"
    "strconv"
    "crypto/tls"

    "github.com/golang/glog"
    "github.com/go-redis/redis/v8"

    "github.com/azsvcbusbench/internal/helpers"
    "github.com/azsvcbusbench/internal/stats"
)

const (
    testIdPropName  = "testId"
    idxPropName     = "senderIdx"
    trackPropName   = "track"
    contentTypeKey  = "content-type"
    bodyKey         = "body"
)

var (
    msgContentType = "application/json"
    strFalse       = strconv.FormatBool( false )
)

func NewAzRedis( )( *AzRedis ) {
    return &AzRedis {
        Index       : 0,
        azRedisCtx : azRedisCtx {
            wg     : &sync.WaitGroup{ },
            stats  : stats.NewStats( nil, nil ),
        },
    }
}

func ( azRedis *AzRedis )initMsgGen( )( err error ) {
    var msgGen *helpers.MsgGen

    if len( azRedis.IpsFile ) > 0 {
        fh, err := os.Open( azRedis.IpsFile )
        if err != nil {
            glog.Fatalf( "failed to open file %v: error %v", azRedis.IpsFile, err )
            return fmt.Errorf( "failed to open file %v: error %v", azRedis.IpsFile, err )
        }

        defer func( ) {
            fh.Close( )
        }( )

        msgGen, err = helpers.InitMsgGen( fh, 0, helpers.Ipv4AddrClassAny, helpers.MsgTypeJson )
    } else {
        msgGen, err = helpers.InitMsgGen( nil, 64, helpers.Ipv4AddrClassAny, helpers.MsgTypeJson )
    }

    if err != nil {
        glog.Fatalf( "failed to initialize message generator" )
        return fmt.Errorf( "failed to initialize message generator" )
    }

    azRedis.msgGen = msgGen
    return nil
}

func ( azRedis *AzRedis )initIdGen( )( err error ) {
    idGen := helpers.NewIdGenerator( )

    if len( azRedis.IdsFile ) > 0 {
        fh, err := os.Open( azRedis.IdsFile )
        if err != nil {
            glog.Fatalf( "failed to open file %v: error %v", azRedis.IdsFile, err )
            return fmt.Errorf( "failed to open file %v: error %v", azRedis.IdsFile, err )
        }

        defer func( ) {
            fh.Close( )
        }( )

        err = idGen.InitIdBlockFromReader( fh )
    } else {
        uuidsLen := azRedis.TotSenders
        if uuidsLen < azRedis.TotReceivers {
            uuidsLen = azRedis.TotReceivers
        }

        err = idGen.InitIdBlock( uuidsLen )
    }

    if err != nil {
        glog.Fatalf( "failed to initialize id generator" )
        return fmt.Errorf( "failed to initialize id generator" )
    }

    azRedis.idGen = idGen
    return nil
}

func ( azRedis *AzRedis )Start( ) {
    azRedis.client = redis.NewClient(
        &redis.Options {
            Addr            :   azRedis.Host,
            Password        :   azRedis.Password,
            WriteTimeout    :   azRedis.SendInterval,
            TLSConfig       :   &tls.Config {
                MinVersion  :   tls.VersionTLS12,
            },
        },
    )

    realDuration := azRedis.Duration + azRedis.WarmupDuration

    ctx, _             := context.WithTimeout( context.Background( ), realDuration )
    azRedis.senderCtx  = ctx

    ctx, cancel := context.WithTimeout( context.Background( ), realDuration + ( 2 * time.Minute ) )
    defer func( ) {
        cancel( )
    }( )
    azRedis.receiverCtx = ctx
    azRedis.statsCtx    = ctx

    err := azRedis.client.Ping( azRedis.senderCtx ).Err( )
    if err != nil {
        glog.Fatalf( "failed to connect with redis instance at %v - %v", azRedis.Host, err )
        return
    } 

    err = azRedis.initMsgGen( )
    if err != nil {
        glog.Fatalf( "failed to initialize message generator: error %v", err )
        return
    }

    err = azRedis.initIdGen( )
    if err != nil {
        glog.Fatalf( "failed to initialize id generator: error %v", err )
        return
    }

    azRedis.stats.SetCtx( azRedis.statsCtx )
    azRedis.stats.SetIds( azRedis.idGen.Block )
    azRedis.stats.SetStatsDumpInterval( azRedis.StatDumpInterval )
    azRedis.stats.StartDumper( )

    if !azRedis.SenderOnly {
        azRedis.wg.Add( azRedis.TotReceivers )
        for i := 0; i < azRedis.TotReceivers; i++ {
            go func( idx int ) {
                defer azRedis.wg.Done( )
                azRedis.startReceiver( idx )
            }( i )
        }
    }

    if !azRedis.ReceiverOnly {
        azRedis.wg.Add( azRedis.TotSenders )
        for i := 0; i < azRedis.TotSenders; i++ {
            go func( idx int ) {
                defer azRedis.wg.Done( )
                azRedis.startSender( idx )
            }( i )
        }
    }

    azRedis.wg.Add( 1 )
    go func( ) {
        defer azRedis.wg.Done( )
        azRedis.trackWarmup( )
    }( )

    azRedis.wg.Wait( )
    azRedis.stats.StopDumper( )
}

func ( azRedis *AzRedis )trackWarmup( ) {
    warmupTimer := time.NewTimer( azRedis.WarmupDuration )

    select {
        case <-warmupTimer.C:
            azRedis.trackTest = true
            return

        case <-azRedis.senderCtx.Done( ):
            warmupTimer.Stop( )
            return
    }
}

func ( azRedis *AzRedis )getSenderIdFromIdx( idx int )( id string, realIdx int, err error ) {
    realIdx = idx + ( azRedis.Index * azRedis.TotSenders )
    if realIdx >= len( azRedis.idGen.Block ) {
        return "", 0, fmt.Errorf( "did not find id for index %v and offset index %v", idx, azRedis.Index )
    }

    return azRedis.idGen.Block[ realIdx ], realIdx, nil
}

func ( azRedis *AzRedis )sendMessage( idx int )( err error ) {
    id, realIdx, err := azRedis.getSenderIdFromIdx( idx )
    if err != nil {
        glog.Errorf( "Failed to get index, error = %v", err )
        return err
    }

    message := map[ string ]interface{ } {
        contentTypeKey  :   &msgContentType,
        trackPropName   :   strconv.FormatBool( azRedis.trackTest ),
        testIdPropName  :   azRedis.TestId,
        idxPropName     :   realIdx,
    }

    msg, key, err := azRedis.msgGen.GetMsgWithKey( nil )
    if err != nil {
        glog.Errorf( "%v: Failed to get message, error = %v", id, err )
        return err
    }

    message[ bodyKey ] = msg

    _, err = azRedis.client.HSet( azRedis.senderCtx, key, message ).Result( )
    if err != nil {
        glog.Errorf( "%v: Failed to send message, error = %v", id, err )
        return err
    }

    if azRedis.trackTest {
        azRedis.stats.UpdateSenderStat( realIdx, 1 )
    }

    return nil
}

func ( azRedis *AzRedis )newSender( idx int )( err error ) {
    return nil
}

func ( azRedis *AzRedis )closeSender( idx int )( err error ) {
    return nil
}

func ( azRedis *AzRedis )startSender( idx int ) {
    err := azRedis.newSender( idx )
    if err != nil {
        return
    }

    defer func( ) {
        azRedis.closeSender( idx )
    }( )

    for {
        err = azRedis.sendMessage( idx )
        if err != nil {
            return
        }

        time.Sleep( azRedis.SendInterval )
    }

    return
}

func ( azRedis *AzRedis )getReceiverIdFromIdx( idx int )( id string, realIdx int, err error ) {
    realIdx = idx + ( azRedis.Index * azRedis.TotReceivers )
    if realIdx >= len( azRedis.idGen.Block ) {
        return "", 0, fmt.Errorf( "did not find id for index %v and offset index %v", idx, azRedis.Index )
    }

    return azRedis.idGen.Block[ realIdx ], realIdx, nil
}

type azSvcMsgCb func( idx int, message map[ string ]string )( err error )

func ( azRedis *AzRedis )receiveMessages( idx int, cb azSvcMsgCb )( err error ) {
    _, _, err = azRedis.getReceiverIdFromIdx( idx )
    if err != nil {
        glog.Errorf( "Failed to get index, error = %v", err )
        return err
    }

    msgKey, err := azRedis.msgGen.GetRandomMsgKey( )
    if err != nil {
        glog.Errorf( "Failed to get message key, error = %v", err )
    }

    message, err := azRedis.client.HGetAll( azRedis.receiverCtx, msgKey ).Result( )
    if err == nil && cb != nil {
        err = cb( idx, message )
        if err != nil {
            return err
        }
    }

    return nil
}

func ( azRedis *AzRedis )receivedMessageCallback( idx int, message map[ string ]string )( err error ) {
    id, realIdx, err := azRedis.getReceiverIdFromIdx( idx )
    if err != nil {
        glog.Errorf( "Failed to get index, error = %v", err )
        return err
    }

    contentType, exists := message[ contentTypeKey ]
    if exists && contentType != msgContentType {
        glog.Errorf( "%v: Ignoring message with unknown content type %v", id, contentType )
        return fmt.Errorf( "%v: Ignoring message with unknown content type %v", id, contentType )
    }

    track, exists := message[ trackPropName ]
    if exists && track == strFalse {
        return nil
    }

    msg, exists := message[ bodyKey ]
    if !exists {
        glog.Errorf( "%v: Failed to get received message body, error = %v", id, err )
        return fmt.Errorf( "%v: Failed to get received message body, error = %v", id, err )
    }

    msgCb := func( msg *helpers.Msg )( err error ) {
        return azRedis.msgGen.ValidateMsg( msg )
    }

    msgList, err := azRedis.msgGen.ParseMsg( [ ]byte( msg ), msgCb )
    if err != nil {
        glog.Errorf( "%v: Failed to parse message, error = %v", id, err )
        return fmt.Errorf( "%v: Failed to parse message, error = %v", id, err )
    }

    testId, exists := message[ testIdPropName ]
    if exists && testId != azRedis.TestId {
        glog.Errorf( "%v: Invalid test id in message application properties", id )
        return fmt.Errorf( "%v: Invalid test id in message application properties", id )
    }

    senderIdxStr, exists := message[ idxPropName ]
    if exists {
        senderIdx, err := strconv.ParseInt( senderIdxStr, 10, 64 )
        if err != nil {
            glog.Errorf( "%v: Invalid sender index in message application properties", id )
            return fmt.Errorf( "%v: Invalid sender index in message application properties", id )
        }

        azRedis.stats.UpdateReceiverStat( realIdx, int( senderIdx ), uint64( msgList.Count ), uint64( msgList.GetLatency( ) ) )
    } else {
        glog.Errorf( "%v: Did not find sender index in message application properties", id )
        return fmt.Errorf( "%v: Did not find sender index in message application properties", id )
    }

    return nil
}

func ( azRedis *AzRedis )newReceiver( idx int )( err error ) {
    return nil
}

func ( azRedis *AzRedis )closeReceiver( idx int )( err error ) {
    return nil
}

func ( azRedis *AzRedis )startReceiver( idx int ) {
    err := azRedis.newReceiver( idx )
    if err != nil {
        return
    }

    defer func( ) {
        azRedis.closeReceiver( idx )
    }( )

    cb := func( idx int, message map[ string ]string )( err error ) {
        return azRedis.receivedMessageCallback( idx, message )
    }

    for {
        err = azRedis.receiveMessages( idx, cb )
        if err != nil {
            break
        }

        time.Sleep( azRedis.ReceiveInterval )
    }

    return
}
