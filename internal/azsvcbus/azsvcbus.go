package azsvcbus

import (
    "sync"
    "time"
    "context"

    "github.com/golang/glog"
    "github.com/google/uuid"
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

func NewAzSvcBus( )( azSvcBus *AzSvcBus ) {
    azSvcBus = &AzSvcBus {
        azSvcBusCtx : azSvcBusCtx {
            wg    : &sync.WaitGroup{ },
        },
    }

    azSvcBus.stats = stats.NewStats( nil, nil )

    msgs, err := helpers.InitMsgs( 64, helpers.Ipv4AddrClassAny, helpers.MsgTypeJson )
    if err != nil {
        glog.Errorf( "Failed to initialize message generator" )
        return nil
    }
    azSvcBus.msgs = msgs

    return azSvcBus
}

func ( azSvcBus *AzSvcBus )Start( ) {
    client, err := azservicebus.NewClientFromConnectionString( azSvcBus.ConnStr, nil )
    if err != nil {
        glog.Errorf( "Failed to setup Azure Service Bus client %v", err )
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

    azSvcBus.stats.SetCtx( azSvcBus.statsCtx )

    uuidsLen := azSvcBus.TotSenders
    if uuidsLen < azSvcBus.TotReceivers {
        uuidsLen = azSvcBus.TotReceivers
    }

    azSvcBus.uuids = make( [ ]string, uuidsLen )
    for i := 0; i < uuidsLen; i++ {
        azSvcBus.uuids[ i ] = uuid.New( ).String( )
    }

    azSvcBus.stats.SetIds( azSvcBus.uuids )
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
    id := azSvcBus.uuids[ idx ]

    sender, err := azSvcBus.client.NewSender( azSvcBus.TopicName, nil )
    if err != nil {
        glog.Errorf( "%v: Failed to create sender, error = %v", id, err )
        azSvcBus.wg.Done( )
        return
    }

    defer func( ) {
        glog.Infof( "%v: Sender done", id )
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
        msg, err := azSvcBus.msgs.GetMsg( )
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
    id := azSvcBus.uuids[ idx ]

    receiver, err := azSvcBus.client.NewReceiverForSubscription( azSvcBus.TopicName, azSvcBus.SubName, nil )
    if err != nil {
        glog.Errorf( "%v: Failed to create receiver, error = %v", id, err )
        azSvcBus.wg.Done( )
        return
    }

    defer func( ) {
        glog.Infof( "%v: Receiver done", id )
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
                    glog.Infof( "%v: Ignoring message from self", id )
                    continue
                }
            }

            msg, err := message.Body( )
            if err != nil {
                glog.Errorf( "%v: Failed to get received message body, error = %v", id, err )
                break
            }

            msgInst, err := azSvcBus.msgs.ParseMsg( msg )
            if err != nil {
                glog.Errorf( "%v: Failed to parse message, error = %v", id, err )
            }

            if senderIdxPropVal, exists := message.ApplicationProperties[ idxPropName ]; exists {
                senderIdx, ok := senderIdxPropVal.( int )
                if ok {
                    azSvcBus.stats.UpdateReceiverStat( idx, senderIdx, 1, uint64( helpers.GetCurTimeStamp( ) - msgInst.TimeStamp ) )
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
