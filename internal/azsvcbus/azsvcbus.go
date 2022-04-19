package azsvcbus

import (
    "sync"
    "time"
    "context"
    "fmt"

    "github.com/golang/glog"
    "github.com/google/uuid"
    "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

const message = "Hello World!"

func NewAzSvcBus( )( *AzSvcBus ) {
    return &AzSvcBus {
        azSvcBusCtx : azSvcBusCtx {
            wg    : &sync.WaitGroup{ },
        },
    }
}

func ( azSvcBus *AzSvcBus )Start( ) {
    client, err := azservicebus.NewClientFromConnectionString( azSvcBus.ConnStr, nil )
    if err != nil {
        glog.Errorf( "Failed to setup Azure Service Bus client %v", err )
        return
    }

    azSvcBus.client = client

    ctx, cancel := context.WithTimeout( context.Background( ), azSvcBus.Duration )
    defer func( ) {
        cancel( )
    }( )

    azSvcBus.ctx = ctx

    uuidsLen := azSvcBus.TotSenders
    if uuidsLen < azSvcBus.TotReceivers {
        uuidsLen = azSvcBus.TotReceivers
    }

    uuids := make( [ ]string, uuidsLen )
    for i := 0; i < azSvcBus.TotSenders; i++ {
        uuids[ i ] = uuid.New( ).String( )
    }

    if !azSvcBus.SenderOnly {
        azSvcBus.wg.Add( azSvcBus.TotReceivers )
        for i := 0; i < azSvcBus.TotReceivers; i++ {
            go azSvcBus.receiveMessage( uuids[ i ] )
        }
    }

    if !azSvcBus.ReceiverOnly {
        azSvcBus.wg.Add( azSvcBus.TotSenders )
        for i := 0; i < azSvcBus.TotSenders; i++ {
            go azSvcBus.sendMessage( uuids[ i ] )
        }
    }

    azSvcBus.wg.Wait( )
}

func ( azSvcBus *AzSvcBus )sendMessage( id string ) {
    sender, err := azSvcBus.client.NewSender( azSvcBus.TopicName, nil )
    if err != nil {
        glog.Errorf( "%v: Failed to create sender, error = %v", id, err )
        azSvcBus.wg.Done( )
        return
    }

    defer func( ) {
        glog.Infof( "%v: Sender done", id )
        sender.Close( azSvcBus.ctx )
        azSvcBus.wg.Done( )
    }( )

    msg := fmt.Sprintf( "%v:%v:%v", time.Now( ).Format( time.UnixDate ), id, message )

    azsvcbusmsg := &azservicebus.Message{
        Body                  : [ ]byte( msg ),
        ApplicationProperties : map[ string ]interface{ }{ azSvcBus.PropName : id },
    }

    for {
        err = sender.SendMessage( azSvcBus.ctx, azsvcbusmsg, nil )
        if err != nil {
            glog.Errorf( "%v: Failed to send message, error = %v", id, err )
            return
        }

        time.Sleep( azSvcBus.SendInterval )
    }

    return
}

func ( azSvcBus *AzSvcBus )receiveMessage( id string ) {
    receiver, err := azSvcBus.client.NewReceiverForSubscription( azSvcBus.TopicName, azSvcBus.SubName, nil )
    if err != nil {
        glog.Errorf( "%v: Failed to create receiver, error = %v", id, err )
        azSvcBus.wg.Done( )
        return
    }

    defer func( ) {
        glog.Infof( "%v: Receiver done", id )
        receiver.Close( azSvcBus.ctx )
        azSvcBus.wg.Done( )
    }( )

    for {
        glog.Infof( "%v: Waiting to receive messages", id )
        messages, err := receiver.PeekMessages( azSvcBus.ctx, 1, nil )
        if err != nil {
            glog.Errorf( "%v: Failed to receive messages, error = %v", id, err )
            return
        }

        for _, message := range messages {
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
                glog.Infof( "%v: Failed to get received message body, error = %v", id, err )
                break
            }

            glog.Infof( "%v: Received message [%v]", id, string( msg ) )
        }

        time.Sleep( azSvcBus.ReceiveInterval )
    }

    return
}
