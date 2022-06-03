package main

import (
    "flag"
    "os"
    "time"
    "strconv"

    "github.com/golang/glog"
    "github.com/azsvcbusbench/internal/azsvcbus"
)

var (
    version     string

    testId         = flag.String( "test-id", "", "Test id" )
    connStr        = flag.String( "conn-string", "", "Connection string to access service bus" )
    topicName      = flag.String( "topic-name", "", "Topic to subscribe to" )
    subName        = flag.String( "subscription-name", "", "Subscription name" )
    propName       = flag.String( "property-name", "senderid", "Property name" )
    totGws         = flag.Int( "total-gateways", 2, "Total simulated gateways" )
    sndIntvl       = flag.Duration( "send-interval", 5 * time.Second, "Interval between successive publish attempts" )
    rcvIntvl       = flag.Duration( "receive-interval", 1 * time.Second, "Interval between successive receive attempts" )
    msgsPerRcv     = flag.Int( "messages-per-receive", 1, "Number of messages to get per receive call" )
    msgsPerSnd     = flag.Int( "messages-per-send", 1, "Number of messages to push per send call" )
    testTime       = flag.Duration( "test-duration", 5 * time.Minute, "Total test time" )
    testWarmupTime = flag.Duration( "test-warmup-time", 1 * time.Minute, "Test warmup time" )
    sndrOnly       = flag.Bool( "sender-only", false, "Enable sender only" )
    rcvrOnly       = flag.Bool( "receiver-only", false, "Enable receiver only" )
    statIntvl      = flag.Duration( "stats-dump-interval", 30 * time.Second, "Interval after statistics will be dumped" )
    ipsFile        = flag.String( "ips-file", "", "File with list of ip addresses to use" )
    idsFile        = flag.String( "ids-file", "", "File with list of ids to use" )
)

func main( ) {
    flag.Parse( )

    err := flag.Lookup( "logtostderr" ).Value.Set( "true" )
    if err != nil {
        glog.Fatalf( "Error setting logtostderr to true: %v", err )
    }

    glog.Infof( "Starting azsvcbusbench %v", version )

    azsvcbusBench := azsvcbus.NewAzSvcBus( )
    if azsvcbusBench == nil {
        glog.Fatalf( "Failed to initialize service bus bench" )
    }

    setupString( &azsvcbusBench.TestId, testId, "AZSVCBUS_TEST_ID" )

    setupString( &azsvcbusBench.ConnStr, connStr, "AZSVCBUS_CONN_STR" )
    if 0 == len( azsvcbusBench.ConnStr ) {
        glog.Fatalf( "Connection string cannot be empty" )
    }

    setupString( &azsvcbusBench.TopicName, topicName, "AZSVCBUS_TOPIC_NAME" )
    setupString( &azsvcbusBench.SubName, subName, "AZSVCBUS_SUB_NAME" )
    setupString( &azsvcbusBench.PropName, propName, "AZSVCBUS_PROP_NAME" )

    setupInt( &azsvcbusBench.TotGateways, totGws, "AZSVCBUS_TOTAL_GATEWAYS" )
    setupInt( &azsvcbusBench.MsgsPerReceive, msgsPerRcv, "AZSVCBUS_MSGS_PER_RECEIVE" )
    setupInt( &azsvcbusBench.MsgsPerSend, msgsPerSnd, "AZSVCBUS_MSGS_PER_SEND" )

    setupBool( &azsvcbusBench.SenderOnly, sndrOnly, "AZSVCBUS_SENDER_ONLY" )
    setupBool( &azsvcbusBench.ReceiverOnly, rcvrOnly, "AZSVCBUS_RECEIVER_ONLY" )

    setupDuration( &azsvcbusBench.Duration, testTime, "AZSVCBUS_TEST_DURATION" )
    setupDuration( &azsvcbusBench.WarmupDuration, testWarmupTime, "AZSVCBUS_TEST_WARMUP_TIME" )
    setupDuration( &azsvcbusBench.SendInterval, sndIntvl, "AZSVCBUS_SEND_INTERVAL" )
    setupDuration( &azsvcbusBench.ReceiveInterval, rcvIntvl, "AZSVCBUS_RECEIVE_INTERVAL" )
    setupDuration( &azsvcbusBench.StatDumpInterval, statIntvl, "AZSVCBUS_STATS_DUMP_INTERVAL" )

    setupString( &azsvcbusBench.IpsFile, ipsFile, "AZSVCBUS_IPS_FILE" )
    setupString( &azsvcbusBench.IdsFile, idsFile, "AZSVCBUS_IDS_FILE" )

    setupInt( &azsvcbusBench.Index, nil, "JOB_COMPLETION_INDEX" )

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
