package main

import (
    "flag"
    "os"
    "time"
    "strconv"

    "github.com/golang/glog"
    "github.com/azsvcbusbench/internal/azsvcbusbench"
)

var (
    version     string

    connStr     = flag.String( "conn-string", "", "Connection string to access service bus" )
    topicName   = flag.String( "topic-name", "", "Topic to subscribe to" )
    subName     = flag.String( "subscription-name", "", "Subscription name" )
    propName    = flag.String( "property-name", "senderid", "Property name" )
    totSndrs    = flag.Int( "total-senders", 2, "Total senders" )
    totRcvrs    = flag.Int( "total-receivers", 2, "Total receivers" )
    sndIntvl    = flag.Duration( "send-interval", 5 * time.Second, "Interval between successive publish attempts" )
    rcvIntvl    = flag.Duration( "receive-interval", 1 * time.Second, "Interval between successive receive attempts" )
    testTime    = flag.Duration( "test-duration", 5 * time.Minute, "Total test time" )
    sndrOnly    = flag.Bool( "sender-only", false, "Enable sender only" )
    rcvrOnly    = flag.Bool( "receiver-only", false, "Enable receiver only" )
)

func main( ) {
    flag.Parse( )

    err := flag.Lookup( "logtostderr" ).Value.Set( "true" )
    if err != nil {
        glog.Fatalf( "Error setting logtostderr to true: %v", err )
    }

    glog.Infof( "Starting azsvcbusbench %v", version )

    azsvcbusBench := azsvcbusbench.NewAzSvcBusBench( )

    setupString( &azsvcbusBench.ConnStr, connStr, "AZSVCBUS_CONN_STR" )
    if 0 == len( azsvcbusBench.ConnStr ) {
        glog.Fatalf( "Connection string cannot be empty" )
    }

    setupString( &azsvcbusBench.TopicName, topicName, "AZSVCBUS_TOPIC_NAME" )
    setupString( &azsvcbusBench.SubName, subName, "AZSVCBUS_SUB_NAME" )
    setupString( &azsvcbusBench.PropName, propName, "AZSVCBUS_PROP_NAME" )

    setupInt( &azsvcbusBench.TotSenders, totSndrs, "AZSVCBUS_TOTAL_SENDERS" )
    setupInt( &azsvcbusBench.TotReceivers, totRcvrs, "AZSVCBUS_TOTAL_RECEIVERS" )

    setupBool( &azsvcbusBench.SenderOnly, sndrOnly, "AZSVCBUS_SENDER_ONLY" )
    setupBool( &azsvcbusBench.ReceiverOnly, rcvrOnly, "AZSVCBUS_RECEIVER_ONLY" )

    setupDuration( &azsvcbusBench.Duration, testTime, "AZSVCBUS_TEST_DURATION" )
    setupDuration( &azsvcbusBench.SendInterval, sndIntvl, "AZSVCBUS_SEND_INTERVAL" )
    setupDuration( &azsvcbusBench.ReceiveInterval, rcvIntvl, "AZSVCBUS_RECEIVE_INTERVAL" )

    glog.Infof( "Starting Azure Service Bus Bench test %+v", azsvcbusBench )
    azsvcbusBench.Start( )
}

func setupString( field, arg *string, envVar string ) {
    envVal := os.Getenv( envVar )
    if len( envVal ) > 0 {
        *field = envVal
        return
    }

    if arg != nil && len( *arg ) > 0 {
        *field = *arg
    }
}

func setupBool( field, arg *bool, envVar string ) {
    envVal := os.Getenv( envVar )
    if len( envVal ) > 0 {
        if boolVal, err := strconv.ParseBool( envVal ); nil == err {
            *field = boolVal
            return
        }
    }

    if arg != nil {
        *field = *arg
    }
}

func setupInt( field, arg *int, envVar string ) {
    envVal := os.Getenv( envVar )
    if len( envVal ) > 0 {
        if uintVal, err := strconv.ParseInt( envVal, 10, 32 ); nil == err {
            *field = int( uintVal )
            return
        }
    }

    if arg != nil {
        *field = *arg
    }
}

func setupDuration( field, arg *time.Duration, envVar string ) {
    envVal := os.Getenv( envVar )
    if len( envVal ) > 0 {
        if durVal, err := time.ParseDuration( envVal ); nil == err {
            *field = durVal
            return
        }
    }

    if arg != nil {
        *field = *arg
    }
}
