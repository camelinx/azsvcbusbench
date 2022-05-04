package azsvcbus

import (
    "sync"
    "time"
    "context"
    "os"
    "fmt"

    "github.com/golang/glog"
    "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
    "github.com/azsvcbusbench/internal/helpers"
    "github.com/azsvcbusbench/internal/stats"
)

const (
    idxPropName = "senderIdx"
)

var (
    msgContentType = "application/json"
)

func NewAzSvcBus( )( *AzSvcBus ) {
    return &AzSvcBus {
        Index       : 0,
        azSvcBusCtx : azSvcBusCtx {
            wg    : &sync.WaitGroup{ },
            stats : stats.NewStats( nil, nil ),
        },
    }
}

func ( azSvcBus *AzSvcBus )initMsgGen( )( err error ) {
    var msgGen *helpers.MsgGen

    if len( azSvcBus.IpsFile ) > 0 {
        fh, err := os.Open( azSvcBus.IpsFile )
        if err != nil {
            glog.Fatalf( "failed to open file %v: error %v", azSvcBus.IpsFile, err )
            return fmt.Errorf( "failed to open file %v: error %v", azSvcBus.IpsFile, err )
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

    azSvcBus.msgGen = msgGen
    return nil
}

func ( azSvcBus *AzSvcBus )initIdGen( )( err error ) {
    idGen := helpers.NewIdGenerator( )

    if len( azSvcBus.IdsFile ) > 0 {
        fh, err := os.Open( azSvcBus.IdsFile )
        if err != nil {
            glog.Fatalf( "failed to open file %v: error %v", azSvcBus.IdsFile, err )
            return fmt.Errorf( "failed to open file %v: error %v", azSvcBus.IdsFile, err )
        }

        defer func( ) {
            fh.Close( )
        }( )

        err = idGen.InitIdBlockFromReader( fh )
    } else {
        uuidsLen := azSvcBus.TotSenders
        if uuidsLen < azSvcBus.TotReceivers {
            uuidsLen = azSvcBus.TotReceivers
        }

        err = idGen.InitIdBlock( uuidsLen )
    }

    if err != nil {
        glog.Fatalf( "failed to initialize id generator" )
        return fmt.Errorf( "failed to initialize id generator" )
    }

    azSvcBus.idGen = idGen
    return nil
}

func ( azSvcBus *AzSvcBus )Start( ) {
    client, err := azservicebus.NewClientFromConnectionString( azSvcBus.ConnStr, nil )
    if err != nil {
        glog.Fatalf( "failed to setup Azure Service Bus client %v", err )
        return
    }

    azSvcBus.client = client

    ctx, _             := context.WithTimeout( context.Background( ), azSvcBus.Duration )
    azSvcBus.senderCtx  = ctx

    ctx, cancel := context.WithTimeout( context.Background( ), azSvcBus.Duration + ( 2 * time.Minute ) )
    defer func( ) {
        cancel( )
    }( )
    azSvcBus.receiverCtx = ctx
    azSvcBus.statsCtx    = ctx

    err = azSvcBus.initMsgGen( )
    if err != nil {
        glog.Fatalf( "failed to initialize message generator: error %v", err )
        return
    }

    err = azSvcBus.initIdGen( )
    if err != nil {
        glog.Fatalf( "failed to initialize id generator: error %v", err )
        return
    }

    azSvcBus.stats.SetCtx( azSvcBus.statsCtx )
    azSvcBus.stats.SetIds( azSvcBus.idGen.Block )
    azSvcBus.stats.SetStatsDumpInterval( azSvcBus.StatDumpInterval )
    azSvcBus.stats.StartDumper( )

    if !azSvcBus.SenderOnly {
        azSvcBus.receivers = make( [ ]*azservicebus.Receiver, azSvcBus.TotReceivers )
        azSvcBus.wg.Add( azSvcBus.TotReceivers )
        for i := 0; i < azSvcBus.TotReceivers; i++ {
            go func( idx int ) {
                defer azSvcBus.wg.Done( )
                azSvcBus.startReceiver( idx )
            }( i )
        }
    }

    if !azSvcBus.ReceiverOnly {
        azSvcBus.senders = make( [ ]*azservicebus.Sender, azSvcBus.TotSenders )
        azSvcBus.wg.Add( azSvcBus.TotSenders )
        for i := 0; i < azSvcBus.TotSenders; i++ {
            go func( idx int ) {
                defer azSvcBus.wg.Done( )
                azSvcBus.startSender( idx )
            }( i )
        }
    }

    azSvcBus.wg.Wait( )
    azSvcBus.stats.StopDumper( )
}

func ( azSvcBus *AzSvcBus )getSenderIdFromIdx( idx int )( id string, realIdx int, err error ) {
    realIdx = idx + ( azSvcBus.Index * azSvcBus.TotSenders )
    if realIdx >= len( azSvcBus.idGen.Block ) {
        return "", 0, fmt.Errorf( "did not find id for index %v and offset index %v", idx, azSvcBus.Index )
    }

    return azSvcBus.idGen.Block[ realIdx ], realIdx, nil
}

func ( azSvcBus *AzSvcBus )sendMessage( idx int )( err error ) {
    id, realIdx, err := azSvcBus.getSenderIdFromIdx( idx )
    if err != nil {
        glog.Errorf( "Failed to get index, error = %v", err )
        return err
    }

    appProps := map[ string ]interface{ }{
        azSvcBus.PropName : id,
        idxPropName       : realIdx,
    }

    azsvcbusmsg := &azservicebus.Message{
        ApplicationProperties   : appProps,
        ContentType             : &msgContentType,
    }

    msg, err := azSvcBus.msgGen.GetMsgN( azSvcBus.MsgsPerSend )
    if err != nil {
        glog.Errorf( "%v: Failed to get message, error = %v", id, err )
        return err
    }

    azsvcbusmsg.Body = msg

    err = azSvcBus.senders[ idx ].SendMessage( azSvcBus.senderCtx, azsvcbusmsg, nil )
    if err != nil {
        glog.Errorf( "%v: Failed to send message, error = %v", id, err )
        return err
    }

    azSvcBus.stats.UpdateSenderStat( realIdx, uint64( azSvcBus.MsgsPerSend ) )

    return nil
}

func ( azSvcBus *AzSvcBus )newSender( idx int )( err error ) {
    if azSvcBus.senders[ idx ] == nil {
        id, _, err := azSvcBus.getSenderIdFromIdx( idx )
        if err != nil {
            glog.Errorf( "Failed to get index, error = %v", err )
            return err
        }

        azSvcBusSender, err := azSvcBus.client.NewSender( azSvcBus.TopicName, nil )
        if err != nil {
            glog.Errorf( "%v: Failed to create sender, error = %v", id, err )
            return err
        }

        azSvcBus.senders[ idx ] = azSvcBusSender
    }

    return nil
}

func ( azSvcBus *AzSvcBus )closeSender( idx int )( err error ) {
    if azSvcBus.senders[ idx ] != nil {
        azSvcBus.senders[ idx ].Close( azSvcBus.senderCtx )
        azSvcBus.senders[ idx ] = nil
    }

    return nil
}

func ( azSvcBus *AzSvcBus )startSender( idx int ) {
    err := azSvcBus.newSender( idx )
    if err != nil {
        return
    }

    defer func( ) {
        azSvcBus.closeSender( idx )
    }( )

    for {
        err = azSvcBus.sendMessage( idx )
        if err != nil {
            return
        }

        time.Sleep( azSvcBus.SendInterval )
    }

    return
}

func ( azSvcBus *AzSvcBus )getReceiverIdFromIdx( idx int )( id string, realIdx int, err error ) {
    realIdx = idx + ( azSvcBus.Index * azSvcBus.TotReceivers )
    if realIdx >= len( azSvcBus.idGen.Block ) {
        return "", 0, fmt.Errorf( "did not find id for index %v and offset index %v", idx, azSvcBus.Index )
    }

    return azSvcBus.idGen.Block[ realIdx ], realIdx, nil
}

type azSvcMsgCb func( idx int, message *azservicebus.ReceivedMessage )( err error )

func ( azSvcBus *AzSvcBus )receiveMessages( idx int, cb azSvcMsgCb )( err error ) {
    id, _, err := azSvcBus.getReceiverIdFromIdx( idx )
    if err != nil {
        glog.Errorf( "Failed to get index, error = %v", err )
        return err
    }

    messages, err := azSvcBus.receivers[ idx ].PeekMessages( azSvcBus.receiverCtx, azSvcBus.MsgsPerReceive, nil )
    if err != nil {
        glog.Errorf( "%v: Failed to receive messages, error = %v", id, err )
        return err
    }

    for _, message := range messages {
        if cb != nil {
            err = cb( idx, message )
            if err != nil {
                return err
            }
        }
    }

    return nil
}

func ( azSvcBus *AzSvcBus )receivedMessageCallback( idx int, message *azservicebus.ReceivedMessage )( err error ) {
    id, realIdx, err := azSvcBus.getReceiverIdFromIdx( idx )
    if err != nil {
        glog.Errorf( "Failed to get index, error = %v", err )
        return err
    }

    if message.ContentType != nil && *message.ContentType != msgContentType {
        glog.Errorf( "%v: Ignoring message with unknown content type %v", id, message.ContentType )
        return fmt.Errorf( "%v: Ignoring message with unknown content type %v", id, message.ContentType )
    }

    propVal, exists := message.ApplicationProperties[ azSvcBus.PropName ]
    if exists {
        sndid, ok := propVal.( string )
        if ok && id == sndid {
            return nil
        }
    }

    msg, err := message.Body( )
    if err != nil {
        glog.Errorf( "%v: Failed to get received message body, error = %v", id, err )
        return fmt.Errorf( "%v: Failed to get received message body, error = %v", id, err )
    }

    msgCb := func( msg *helpers.Msg )( err error ) {
        return azSvcBus.msgGen.ValidateMsg( msg )
    }

    msgList, err := azSvcBus.msgGen.ParseMsg( msg, msgCb )
    if err != nil {
        glog.Errorf( "%v: Failed to parse message, error = %v", id, err )
        return fmt.Errorf( "%v: Failed to parse message, error = %v", id, err )
    }

    if senderIdxPropVal, exists := message.ApplicationProperties[ idxPropName ]; exists {
        senderIdx, ok := senderIdxPropVal.( int64 )
        if ok {
            azSvcBus.stats.UpdateReceiverStat( realIdx, int( senderIdx ), uint64( msgList.Count ), uint64( msgList.GetLatency( ) ) )
        } else {
            glog.Errorf( "%v: Invalid sender index in message application properties", id )
            return fmt.Errorf( "%v: Invalid sender index in message application properties", id )
        }
    } else {
        glog.Errorf( "%v: Did not find sender index in message application properties", id )
        return fmt.Errorf( "%v: Did not find sender index in message application properties", id )
    }

    return nil
}

func ( azSvcBus *AzSvcBus )newReceiver( idx int )( err error ) {
    if azSvcBus.receivers[ idx ] == nil {
        id, _, err := azSvcBus.getReceiverIdFromIdx( idx )
        if err != nil {
            glog.Errorf( "Failed to get index, error = %v", err )
            return err
        }

        azSvcBusReceiver, err := azSvcBus.client.NewReceiverForSubscription( azSvcBus.TopicName, azSvcBus.SubName, nil )
        if err != nil {
            glog.Errorf( "%v: Failed to create receiver, error = %v", id, err )
            return err
        }

        azSvcBus.receivers[ idx ] = azSvcBusReceiver
    }

    return nil
}

func ( azSvcBus *AzSvcBus )closeReceiver( idx int )( err error ) {
    if azSvcBus.receivers[ idx ] != nil {
        azSvcBus.receivers[ idx ].Close( azSvcBus.receiverCtx )
        azSvcBus.receivers[ idx ] = nil
    }

    return nil
}

func ( azSvcBus *AzSvcBus )startReceiver( idx int ) {
    err := azSvcBus.newReceiver( idx )
    if err != nil {
        return
    }

    defer func( ) {
        azSvcBus.closeReceiver( idx )
    }( )

    cb := func( idx int, message *azservicebus.ReceivedMessage )( err error ) {
        return azSvcBus.receivedMessageCallback( idx, message )
    }

    for {
        err = azSvcBus.receiveMessages( idx, cb )
        if err != nil {
            break
        }

        time.Sleep( azSvcBus.ReceiveInterval )
    }

    return
}
