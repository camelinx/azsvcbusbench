package main

import (
    "flag"
    "os"
    "time"
    "strconv"

    "github.com/golang/glog"
    "github.com/azsvcbusbench/internal/azevhub"
)

var (
    version     string

    testId         = flag.String( "test-id", "", "Test id" )
    connStr        = flag.String( "conn-string", "", "Connection string to access event hub" )
    nameSpace      = flag.String( "namespace", "", "Name Space" )
    topicName      = flag.String( "topic-name", "", "Topic to subscribe to" )
    consumerGrpPfx = flag.String( "consumer-group-prefix", "", "Consumer Group Prefix" )
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

    glog.Infof( "Starting azevhubbench %v", version )

    azevhubBench := azevhub.NewAzEvHub( )
    if azevhubBench == nil {
        glog.Fatalf( "Failed to initialize event hub bench" )
    }

    setupString( &azevhubBench.TestId, testId, "AZEVHUB_TEST_ID" )

    setupString( &azevhubBench.ConnStr, connStr, "AZEVHUB_CONN_STR" )
    if 0 == len( azevhubBench.ConnStr ) {
        glog.Fatalf( "Connection string cannot be empty" )
    }

    setupString( &azevhubBench.NameSpace, nameSpace, "AZEVHUB_NAME_SPACE" )
    setupString( &azevhubBench.TopicName, topicName, "AZEVHUB_TOPIC_NAME" )
    setupString( &azevhubBench.ConsumerGroupPrefix, consumerGrpPfx, "AZEVHUB_CONSUMER_GROUP_PREFIX" )
    setupString( &azevhubBench.PropName, propName, "AZEVHUB_PROP_NAME" )

    setupInt( &azevhubBench.TotSenders, totGws, "AZEVHUB_TOTAL_GATEWAYS" )
    setupInt( &azevhubBench.TotReceivers, totGws, "AZEVHUB_TOTAL_GATEWAYS" )
    setupInt( &azevhubBench.MsgsPerReceive, msgsPerRcv, "AZEVHUB_MSGS_PER_RECEIVE" )
    setupInt( &azevhubBench.MsgsPerSend, msgsPerSnd, "AZEVHUB_MSGS_PER_SEND" )

    setupBool( &azevhubBench.SenderOnly, sndrOnly, "AZEVHUB_SENDER_ONLY" )
    setupBool( &azevhubBench.ReceiverOnly, rcvrOnly, "AZEVHUB_RECEIVER_ONLY" )

    setupDuration( &azevhubBench.Duration, testTime, "AZEVHUB_TEST_DURATION" )
    setupDuration( &azevhubBench.WarmupDuration, testWarmupTime, "AZSVCBUS_TEST_WARMUP_TIME" )
    setupDuration( &azevhubBench.SendInterval, sndIntvl, "AZEVHUB_SEND_INTERVAL" )
    setupDuration( &azevhubBench.ReceiveInterval, rcvIntvl, "AZEVHUB_RECEIVE_INTERVAL" )
    setupDuration( &azevhubBench.StatDumpInterval, statIntvl, "AZEVHUB_STATS_DUMP_INTERVAL" )

    setupString( &azevhubBench.IpsFile, ipsFile, "AZEVHUB_IPS_FILE" )
    setupString( &azevhubBench.IdsFile, idsFile, "AZEVHUB_IDS_FILE" )

    setupInt( &azevhubBench.Index, nil, "JOB_COMPLETION_INDEX" )

    glog.Infof( "Starting Azure Event Hub Bench test %+v", azevhubBench )
    azevhubBench.Start( )
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
