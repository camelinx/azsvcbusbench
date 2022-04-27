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

    err = azSvcBus.startWarmup( )
    if err != nil {
        glog.Fatalf( "warmup failed: error %v", err )
        return
    }

    azSvcBus.stats.SetCtx( azSvcBus.statsCtx )
    azSvcBus.stats.SetIds( azSvcBus.idGen.Block )
    azSvcBus.stats.SetStatsDumpInterval( azSvcBus.StatDumpInterval )
    azSvcBus.stats.StartDumper( )

    if !azSvcBus.SenderOnly {
        azSvcBus.wg.Add( azSvcBus.TotReceivers )
        for i := 0; i < azSvcBus.TotReceivers; i++ {
            go func( ) {
                defer azSvcBus.wg.Done( )
                azSvcBus.startReceiver( i )
            }( )
        }
    }

    if !azSvcBus.ReceiverOnly {
        azSvcBus.wg.Add( azSvcBus.TotSenders )
        for i := 0; i < azSvcBus.TotSenders; i++ {
            go func( ) {
                defer azSvcBus.wg.Done( )
                azSvcBus.startSender( i )
            }( )
        }
    }

    azSvcBus.wg.Wait( )
    azSvcBus.stats.StopDumper( )
}

func ( azSvcBus *AzSvcBus )startWarmup( )( err error ) {
    err = azSvcBus.newSender( 0 )
    if err != nil {
        return err
    }

    err = azSvcBus.newReceiver( 0 )
    if err != nil {
        return err
    }

    for i := 0; i < 5; i++ {
        err = azSvcBus.sendMessage( 0 )
        if err != nil {
            return err
        }
    }

    cb := func( idx int, message *azservicebus.ReceivedMessage )( err error ) {
        // Do nothing. This is just a warm up
        return nil
    }

    for i := 0; i < 5; i++ {
        err = azSvcBus.receiveMessages( 0, cb )
        if err != nil {
            return err
        }
    }

    return nil
}

func ( azSvcBus *AzSvcBus )sendMessage( idx int )( err error ) {
    id := azSvcBus.idGen.Block[ idx ]

    appProps := map[ string ]interface{ }{
        azSvcBus.PropName : id,
        idxPropName       : idx,
    }

    azsvcbusmsg := &azservicebus.Message{
        ApplicationProperties   : appProps,
        ContentType             : &msgContentType,
    }

    msg, err := azSvcBus.msgGen.GetMsg( )
    if err != nil {
        glog.Errorf( "%v: Failed to get message, error = %v", id, err )
        return err
    }

    azsvcbusmsg.Body = msg

    err = azSvcBus.sender.SendMessage( azSvcBus.senderCtx, azsvcbusmsg, nil )
    if err != nil {
        glog.Errorf( "%v: Failed to send message, error = %v", id, err )
        return err
    }

    return nil
}

func ( azSvcBus *AzSvcBus )newSender( idx int )( err error ) {
    if azSvcBus.sender == nil {
        id := azSvcBus.idGen.Block[ idx ]

        azSvcBusSender, err := azSvcBus.client.NewSender( azSvcBus.TopicName, nil )
        if err != nil {
            glog.Errorf( "%v: Failed to create sender, error = %v", id, err )
            return err
        }

        azSvcBus.sender = azSvcBusSender
    }

    return nil
}

func ( azSvcBus *AzSvcBus )closeSender( idx int )( err error ) {
    azSvcBus.sender.Close( azSvcBus.senderCtx )
    azSvcBus.sender = nil
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

        azSvcBus.stats.UpdateSenderStat( idx, 1 )
        time.Sleep( azSvcBus.SendInterval )
    }

    return
}

type azSvcMsgCb func( idx int, message *azservicebus.ReceivedMessage )( err error )

func ( azSvcBus *AzSvcBus )receiveMessages( idx int, cb azSvcMsgCb )( err error ) {
    id := azSvcBus.idGen.Block[ idx ]

    messages, err := azSvcBus.receiver.PeekMessages( azSvcBus.receiverCtx, 1, nil )
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
    id := azSvcBus.idGen.Block[ idx ]

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

    msgInst, err := azSvcBus.msgGen.ParseMsg( msg )
    if err != nil {
        glog.Errorf( "%v: Failed to parse message, error = %v", id, err )
        return fmt.Errorf( "%v: Failed to parse message, error = %v", id, err )
    }

    if senderIdxPropVal, exists := message.ApplicationProperties[ idxPropName ]; exists {
        senderIdx, ok := senderIdxPropVal.( int64 )
        if ok {
            azSvcBus.stats.UpdateReceiverStat( idx, int( senderIdx ), 1, uint64( msgInst.GetLatency( ) ) )
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
    if azSvcBus.receiver != nil {
        id := azSvcBus.idGen.Block[ idx ]

        azSvcBusReceiver, err := azSvcBus.client.NewReceiverForSubscription( azSvcBus.TopicName, azSvcBus.SubName, nil )
        if err != nil {
            glog.Errorf( "%v: Failed to create receiver, error = %v", id, err )
            return err
        }

        azSvcBus.receiver = azSvcBusReceiver
    }

    return nil
}

func ( azSvcBus *AzSvcBus )closeReceiver( idx int )( err error ) {
    azSvcBus.receiver.Close( azSvcBus.receiverCtx )
    azSvcBus.receiver = nil
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
