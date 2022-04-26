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

    azSvcBus.stats.SetCtx( azSvcBus.statsCtx )
    azSvcBus.stats.SetIds( azSvcBus.idGen.Block )
    azSvcBus.stats.SetStatsDumpInterval( azSvcBus.StatDumpInterval )
    azSvcBus.stats.StartDumper( )

    if !azSvcBus.SenderOnly {
        azSvcBus.wg.Add( azSvcBus.TotReceivers )
        for i := 0; i < azSvcBus.TotReceivers; i++ {
            go azSvcBus.receiveMessage( i )
        }
    }

    if !azSvcBus.ReceiverOnly {
        azSvcBus.wg.Add( azSvcBus.TotSenders )
        for i := 0; i < azSvcBus.TotSenders; i++ {
            go azSvcBus.sendMessage( i )
        }
    }

    azSvcBus.wg.Wait( )
    azSvcBus.stats.StopDumper( )
}

func ( azSvcBus *AzSvcBus )sendMessage( idx int ) {
    id := azSvcBus.idGen.Block[ idx ]

    sender, err := azSvcBus.client.NewSender( azSvcBus.TopicName, nil )
    if err != nil {
        glog.Errorf( "%v: Failed to create sender, error = %v", id, err )
        azSvcBus.wg.Done( )
        return
    }

    defer func( ) {
        sender.Close( azSvcBus.senderCtx )
        azSvcBus.wg.Done( )
    }( )

    appProps := map[ string ]interface{ }{
        azSvcBus.PropName : id,
        idxPropName       : idx,
    }

    azsvcbusmsg := &azservicebus.Message{
        ApplicationProperties   : appProps,
        ContentType             : &msgContentType,
    }

    for {
        msg, err := azSvcBus.msgGen.GetMsg( )
        if err != nil {
            glog.Errorf( "%v: Failed to get message, error = %v", id, err )
            break
        }

        azsvcbusmsg.Body = msg

        err = sender.SendMessage( azSvcBus.senderCtx, azsvcbusmsg, nil )
        if err != nil {
            glog.Errorf( "%v: Failed to send message, error = %v", id, err )
            break
        }

        azSvcBus.stats.UpdateSenderStat( idx, 1 )

        time.Sleep( azSvcBus.SendInterval )
    }

    return
}

func ( azSvcBus *AzSvcBus )receiveMessage( idx int ) {
    id := azSvcBus.idGen.Block[ idx ]

    receiver, err := azSvcBus.client.NewReceiverForSubscription( azSvcBus.TopicName, azSvcBus.SubName, nil )
    if err != nil {
        glog.Errorf( "%v: Failed to create receiver, error = %v", id, err )
        azSvcBus.wg.Done( )
        return
    }

    defer func( ) {
        receiver.Close( azSvcBus.receiverCtx )
        azSvcBus.wg.Done( )
    }( )

    for {
        messages, err := receiver.PeekMessages( azSvcBus.receiverCtx, 1, nil )
        if err != nil {
            glog.Errorf( "%v: Failed to receive messages, error = %v", id, err )
            return
        }

        for _, message := range messages {
            if message.ContentType != nil && *message.ContentType != msgContentType {
                glog.Errorf( "%v: Ignoring message with unknown content type %v", id, message.ContentType )
                continue
            }

            propVal, exists := message.ApplicationProperties[ azSvcBus.PropName ]
            if exists {
                sndid, ok := propVal.( string )
                if ok && id == sndid {
                    continue
                }
            }

            msg, err := message.Body( )
            if err != nil {
                glog.Errorf( "%v: Failed to get received message body, error = %v", id, err )
                break
            }

            msgInst, err := azSvcBus.msgGen.ParseMsg( msg )
            if err != nil {
                glog.Errorf( "%v: Failed to parse message, error = %v", id, err )
            }

            if senderIdxPropVal, exists := message.ApplicationProperties[ idxPropName ]; exists {
                senderIdx, ok := senderIdxPropVal.( int64 )
                if ok {
                    azSvcBus.stats.UpdateReceiverStat( idx, int( senderIdx ), 1, uint64( msgInst.GetLatency( ) ) )
                } else {
                    glog.Errorf( "%v: Invalid sender index in message application properties", id )
                }
            } else {
                glog.Errorf( "%v: Did not find sender index in message application properties", id )
            }
        }

        time.Sleep( azSvcBus.ReceiveInterval )
    }

    return
}
