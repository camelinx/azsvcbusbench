package azsvcbus

import (
    "os"
    "sync"
    "time"
    "context"
    "fmt"

    "github.com/golang/glog"
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
        glog.Errorf( "Failed to setup Azure Service Bus client %v\n", err )
        return
    }

    azSvcBus.client = client

    ctx, cancel := context.WithTimeout( context.Background( ), azSvcBus.Duration )
    defer func( ) {
        cancel( )
    }( )

    azSvcBus.ctx = ctx

    if !azSvcBus.SenderOnly {
        azSvcBus.wg.Add( azSvcBus.TotReceivers )
        for i := 1; i <= azSvcBus.TotReceivers; i++ {
            go azSvcBus.receiveMessage( i )
        }
    }

    if !azSvcBus.ReceiverOnly {
        azSvcBus.wg.Add( azSvcBus.TotSenders )
        for i := 1; i <= azSvcBus.TotSenders; i++ {
            go azSvcBus.sendMessage( i )
        }
    }

    azSvcBus.wg.Wait( )
}

func ( azSvcBus *AzSvcBus )getId( id int )( idStr string ) {
    return fmt.Sprintf( "%v:%v", id, os.Getpid( ) )
}

func ( azSvcBus *AzSvcBus )sendMessage( id int ) {
    sender, err := azSvcBus.client.NewSender( azSvcBus.TopicName, nil )
    if err != nil {
        glog.Errorf( "%v: Failed to create sender, error = %v\n", id, err )
        azSvcBus.wg.Done( )
        return
    }

    defer func( ) {
        glog.Infof( "%v: Sender done\n", id )
        sender.Close( azSvcBus.ctx )
        azSvcBus.wg.Done( )
    }( )

    msg := fmt.Sprintf( "%v:%v:%v:%v", os.Getpid( ), time.Now( ).Format( time.UnixDate ), id, message )

    azsvcbusmsg := &azservicebus.Message{
        Body                  : [ ]byte( msg ),
        ApplicationProperties : map[ string ]interface{ }{ azSvcBus.PropName : azSvcBus.getId( id ) },
    }

    for {
        err = sender.SendMessage( azSvcBus.ctx, azsvcbusmsg, nil )
        if err != nil {
            glog.Errorf( "%v: Failed to send message, error = %v\n", id, err )
            return
        }

        time.Sleep( azSvcBus.SendInterval )
    }

    return
}

func ( azSvcBus *AzSvcBus )receiveMessage( id int ) {
    receiver, err := azSvcBus.client.NewReceiverForSubscription( azSvcBus.TopicName, azSvcBus.SubName, nil )
    if err != nil {
        glog.Errorf( "%v: Failed to create receiver, error = %v\n", id, err )
        azSvcBus.wg.Done( )
        return
    }

    defer func( ) {
        fmt.Printf( "%v: Receiver done\n", id )
        receiver.Close( azSvcBus.ctx )
        azSvcBus.wg.Done( )
    }( )

    rcvid := azSvcBus.getId( id )

    for {
        glog.Infof( "%v: Waiting to receive messages\n", id )
        messages, err := receiver.PeekMessages( azSvcBus.ctx, 1, nil )
        if err != nil {
            glog.Errorf( "%v: Failed to receive messages, error = %v\n", id, err )
            return
        }

        for _, message := range messages {
            propVal, exists := message.ApplicationProperties[ azSvcBus.PropName ]
            if exists {
                sndid, ok := propVal.( string )
                if ok && rcvid == sndid {
                    continue
                }
            }

            msg, err := message.Body( )
            if err != nil {
                fmt.Printf( "%v: Failed to get received message body, error = %v\n", id, err )
                break
            }

            fmt.Printf( "%v: Received message [%v]\n", id, string( msg ) )
        }

        time.Sleep( azSvcBus.ReceiveInterval )
    }

    return
}
