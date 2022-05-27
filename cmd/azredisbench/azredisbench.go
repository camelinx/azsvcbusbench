package main

import (
    "flag"
    "os"
    "time"
    "strconv"

    "github.com/golang/glog"
    "github.com/azsvcbusbench/internal/azredis"
)

var (
    version     string

    testId         = flag.String( "test-id", "", "Test id" )
    host           = flag.String( "host", "", "Host to access redis" )
    password       = flag.String( "password", "", "Password" )
    propName       = flag.String( "property-name", "senderid", "Property name" )
    totGws         = flag.Int( "total-gateways", 2, "Total simulated gateways" )
    sndIntvl       = flag.Duration( "send-interval", 5 * time.Second, "Interval between successive publish attempts" )
    rcvIntvl       = flag.Duration( "receive-interval", 1 * time.Second, "Interval between successive receive attempts" )
    rcvRetries     = flag.Int( "receive-retries", 4, "No of times to attempt reading a key" )
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

    glog.Infof( "Starting azredisbench %v", version )

    azredisBench := azredis.NewAzRedis( )
    if azredisBench == nil {
        glog.Fatalf( "Failed to initialize redis bench" )
    }

    setupString( &azredisBench.TestId, testId, "AZREDIS_TEST_ID" )

    setupString( &azredisBench.Host, host, "AZREDIS_HOST" )
    if 0 == len( azredisBench.Host ) {
        glog.Fatalf( "Host cannot be empty" )
    }

    setupString( &azredisBench.Password, password, "AZREDIS_PASSWD" )
    if 0 == len( azredisBench.Password ) {
        glog.Fatalf( "Password cannot be empty" )
    }

    setupString( &azredisBench.PropName, propName, "AZREDIS_PROP_NAME" )

    setupInt( &azredisBench.TotSenders, totGws, "AZREDIS_TOTAL_GATEWAYS" )
    setupInt( &azredisBench.TotReceivers, totGws, "AZREDIS_TOTAL_GATEWAYS" )

    azredisBench.MsgsPerReceive = 1
    azredisBench.MsgsPerSend    = 1

    setupBool( &azredisBench.SenderOnly, sndrOnly, "AZREDIS_SENDER_ONLY" )
    setupBool( &azredisBench.ReceiverOnly, rcvrOnly, "AZREDIS_RECEIVER_ONLY" )

    setupInt( &azredisBench.ReceiveRetries, rcvRetries, "AZREDIS_RECEIVE_RETRIES" )

    setupDuration( &azredisBench.Duration, testTime, "AZREDIS_TEST_DURATION" )
    setupDuration( &azredisBench.WarmupDuration, testWarmupTime, "AZSVCBUS_TEST_WARMUP_TIME" )
    setupDuration( &azredisBench.SendInterval, sndIntvl, "AZREDIS_SEND_INTERVAL" )
    setupDuration( &azredisBench.ReceiveInterval, rcvIntvl, "AZREDIS_RECEIVE_INTERVAL" )
    setupDuration( &azredisBench.StatDumpInterval, statIntvl, "AZREDIS_STATS_DUMP_INTERVAL" )

    setupString( &azredisBench.IpsFile, ipsFile, "AZREDIS_IPS_FILE" )
    setupString( &azredisBench.IdsFile, idsFile, "AZREDIS_IDS_FILE" )

    setupInt( &azredisBench.Index, nil, "JOB_COMPLETION_INDEX" )

    glog.Infof( "Starting Azure Redis Bench test %+v", azredisBench )
    azredisBench.Start( )
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
