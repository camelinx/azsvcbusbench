package azsvcbusbench

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

func NewAzSvcBusBench( )( *AzSvcBusBench ) {
    return &AzSvcBusBench {
        azSvcBusBenchCtx : azSvcBusBenchCtx {
            wg    : &sync.WaitGroup{ },
        },
    }
}

func ( azSvcBusBench *AzSvcBusBench )Start( ) {
    client, err := azservicebus.NewClientFromConnectionString( azSvcBusBench.ConnStr, nil )
    if err != nil {
        glog.Errorf( "Failed to setup Azure Service Bus client %v\n", err )
	    return
    }

    azSvcBusBench.client = client

    ctx, cancel := context.WithTimeout( context.Background( ), azSvcBusBench.Duration )
    defer func( ) {
        cancel( )
    }( )

    azSvcBusBench.ctx = ctx

    if !azSvcBusBench.SenderOnly {
        azSvcBusBench.wg.Add( azSvcBusBench.TotReceivers )
	for i := 1; i <= azSvcBusBench.TotReceivers; i++ {
            go azSvcBusBench.receiveMessage( i )
        }
    }

    if !azSvcBusBench.ReceiverOnly {
        azSvcBusBench.wg.Add( azSvcBusBench.TotSenders )
	for i := 1; i <= azSvcBusBench.TotSenders; i++ {
            go azSvcBusBench.sendMessage( i )
        }
    }

    azSvcBusBench.wg.Wait( )
}

func ( azSvcBusBench *AzSvcBusBench )getId( id int )( idStr string ) {
    return fmt.Sprintf( "%v:%v", id, os.Getpid( ) )
}

func ( azSvcBusBench *AzSvcBusBench )sendMessage( id int ) {
    sender, err := azSvcBusBench.client.NewSender( azSvcBusBench.TopicName, nil )
    if err != nil {
        glog.Errorf( "%v: Failed to create sender, error = %v\n", id, err )
        azSvcBusBench.wg.Done( )
        return
    }

    defer func( ) {
        glog.Infof( "%v: Sender done\n", id )
        sender.Close( azSvcBusBench.ctx )
        azSvcBusBench.wg.Done( )
    }( )

    msg := fmt.Sprintf( "%v:%v:%v:%v", os.Getpid( ), time.Now( ).Format( time.UnixDate ), id, message )

    azsvcbusmsg := &azservicebus.Message{
        Body                  : [ ]byte( msg ),
        ApplicationProperties : map[ string ]interface{ }{ azSvcBusBench.PropName : azSvcBusBench.getId( id ) },
    }

    for {
        err = sender.SendMessage( azSvcBusBench.ctx, azsvcbusmsg, nil )
        if err != nil {
            glog.Errorf( "%v: Failed to send message, error = %v\n", id, err )
            return
        }

	time.Sleep( azSvcBusBench.SendInterval )
    }

    return
}

func ( azSvcBusBench *AzSvcBusBench )receiveMessage( id int ) {
    receiver, err := azSvcBusBench.client.NewReceiverForSubscription( azSvcBusBench.TopicName, azSvcBusBench.SubName, nil )
    if err != nil {
        glog.Errorf( "%v: Failed to create receiver, error = %v\n", id, err )
	azSvcBusBench.wg.Done( )
        return
    }

    defer func( ) {
        fmt.Printf( "%v: Receiver done\n", id )
        receiver.Close( azSvcBusBench.ctx )
        azSvcBusBench.wg.Done( )
    }( )

    rcvid := azSvcBusBench.getId( id )

    for {
        glog.Infof( "%v: Waiting to receive messages\n", id )
        messages, err := receiver.PeekMessages( azSvcBusBench.ctx, 1, nil )
        if err != nil {
            glog.Errorf( "%v: Failed to receive messages, error = %v\n", id, err )
            return
        }

        for _, message := range messages {
            propVal, exists := message.ApplicationProperties[ azSvcBusBench.PropName ]
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

	time.Sleep( azSvcBusBench.ReceiveInterval )
    }

    return
}
