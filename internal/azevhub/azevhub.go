package azevhub

import (
    "sync"
    "time"
    "context"
    "os"
    "fmt"
    "strconv"

    "github.com/golang/glog"

    evhub "github.com/Azure/azure-event-hubs-go/v3"
    evhub_persist "github.com/Azure/azure-event-hubs-go/v3/persist"

    "github.com/azsvcbusbench/internal/helpers"
    "github.com/azsvcbusbench/internal/stats"
)

const (
    testIdPropName  = "testId"
    idxPropName     = "senderIdx"
    trackPropName   = "track"
)

var (
    msgContentType = "application/json"
    strFalse       = strconv.FormatBool( false )
)

func NewAzEvHub( )( *AzEvHub ) {
    return &AzEvHub {
        Index       : 0,
        azEvHubCtx : azEvHubCtx {
            wg     : &sync.WaitGroup{ },
            stats  : stats.NewStats( nil, nil ),
        },
    }
}

func ( azEvHub *AzEvHub )initMsgGen( )( err error ) {
    var msgGen *helpers.MsgGen

    if len( azEvHub.IpsFile ) > 0 {
        fh, err := os.Open( azEvHub.IpsFile )
        if err != nil {
            glog.Fatalf( "failed to open file %v: error %v", azEvHub.IpsFile, err )
            return fmt.Errorf( "failed to open file %v: error %v", azEvHub.IpsFile, err )
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

    azEvHub.msgGen = msgGen
    return nil
}

func ( azEvHub *AzEvHub )initIdGen( )( err error ) {
    idGen := helpers.NewIdGenerator( )

    if len( azEvHub.IdsFile ) > 0 {
        fh, err := os.Open( azEvHub.IdsFile )
        if err != nil {
            glog.Fatalf( "failed to open file %v: error %v", azEvHub.IdsFile, err )
            return fmt.Errorf( "failed to open file %v: error %v", azEvHub.IdsFile, err )
        }

        defer func( ) {
            fh.Close( )
        }( )

        err = idGen.InitIdBlockFromReader( fh )
    } else {
        err = idGen.InitIdBlock( azEvHub.TotGateways )
    }

    if err != nil {
        glog.Fatalf( "failed to initialize id generator" )
        return fmt.Errorf( "failed to initialize id generator" )
    }

    azEvHub.idGen = idGen
    return nil
}

func ( azEvHub *AzEvHub )Read( nameSpace, name, consumerGroup, partitionId string )( evhub_persist.Checkpoint, error ) {
    return azEvHub.persister.Read( nameSpace, name, consumerGroup, partitionId )
}

func ( azEvHub *AzEvHub )Write( nameSpace, name, consumerGroup, partitionId string, checkPoint evhub_persist.Checkpoint )( error ) {
    return azEvHub.persister.Write( nameSpace, name, consumerGroup, partitionId, checkPoint )
}

func ( azEvHub *AzEvHub )setupCheckPointPersister( )( evhub_persist.CheckpointPersister, error ) {
    if len( azEvHub.PersistDir ) > 0 {
        return evhub_persist.NewFilePersister( azEvHub.PersistDir )
    }

    persister := evhub_persist.NewMemoryPersister( )
    return persister, nil
}

func ( azEvHub *AzEvHub )Start( ) {
    persister, err := azEvHub.setupCheckPointPersister( )
    if err != nil {
        glog.Fatalf( "failed to initialize checkpoint persister %v", err )
        return
    }

    azEvHub.persister = persister

    hub, err := evhub.NewHubFromConnectionString( azEvHub.ConnStr, evhub.HubWithOffsetPersistence( azEvHub ) )
    if err != nil {
        glog.Fatalf( "failed to setup event hub %v", err )
        return
    }

    azEvHub.hub = hub

    realDuration := azEvHub.Duration + azEvHub.WarmupDuration

    ctx, _             := context.WithTimeout( context.Background( ), realDuration )
    azEvHub.senderCtx  = ctx

    ctx, cancel := context.WithTimeout( context.Background( ), realDuration + ( 2 * time.Minute ) )
    defer func( ) {
        cancel( )
    }( )
    azEvHub.receiverCtx = ctx
    azEvHub.statsCtx    = ctx

    err = azEvHub.initMsgGen( )
    if err != nil {
        glog.Fatalf( "failed to initialize message generator: error %v", err )
        return
    }

    err = azEvHub.initIdGen( )
    if err != nil {
        glog.Fatalf( "failed to initialize id generator: error %v", err )
        return
    }

    azEvHub.stats.SetCtx( azEvHub.statsCtx )
    azEvHub.stats.SetIds( azEvHub.idGen.Block )
    azEvHub.stats.SetStatsDumpInterval( azEvHub.StatDumpInterval )
    azEvHub.stats.StartDumper( )

    if !azEvHub.SenderOnly {
        azEvHub.receiversChan  = make( [ ]chan error, azEvHub.TotGateways )
        azEvHub.consumerGroups = make( [ ]string, azEvHub.TotGateways )
        azEvHub.wg.Add( azEvHub.TotGateways )
        for i := 0; i < azEvHub.TotGateways; i++ {
            azEvHub.receiversChan[ i ] = make( chan error )

            go func( idx int ) {
                defer azEvHub.wg.Done( )
                azEvHub.startReceiver( idx )
            }( i )

            receiverErr := <-azEvHub.receiversChan[ i ]
            if receiverErr != nil {
                glog.Fatalf( "failed to start receiver: %v", receiverErr )
            }
        }
    }

    if !azEvHub.ReceiverOnly {
        azEvHub.wg.Add( azEvHub.TotGateways )
        for i := 0; i < azEvHub.TotGateways; i++ {
            go func( idx int ) {
                defer azEvHub.wg.Done( )
                azEvHub.startSender( idx )
            }( i )
        }
    }

    azEvHub.wg.Add( 1 )
    go func( ) {
        defer azEvHub.wg.Done( )
        azEvHub.trackWarmup( )
    }( )

    azEvHub.wg.Wait( )
    azEvHub.stats.StopDumper( )
}

func ( azEvHub *AzEvHub )trackWarmup( ) {
    warmupTimer := time.NewTimer( azEvHub.WarmupDuration )

    select {
        case <-warmupTimer.C:
            azEvHub.trackTest = true
            return

        case <-azEvHub.senderCtx.Done( ):
            warmupTimer.Stop( )
            return
    }
}

func ( azEvHub *AzEvHub )getSenderIdFromIdx( idx int )( id string, realIdx int, err error ) {
    realIdx = idx + ( azEvHub.Index * azEvHub.TotGateways )
    if realIdx >= len( azEvHub.idGen.Block ) {
        return "", 0, fmt.Errorf( "did not find id for index %v and offset index %v", idx, azEvHub.Index )
    }

    return azEvHub.idGen.Block[ realIdx ], realIdx, nil
}

func ( azEvHub *AzEvHub )sendMessage( idx int )( err error ) {
    id, realIdx, err := azEvHub.getSenderIdFromIdx( idx )
    if err != nil {
        glog.Errorf( "Failed to get index, error = %v", err )
        return err
    }

    appProps := map[ string ]interface{ }{
        azEvHub.PropName  : id,
        testIdPropName    : azEvHub.TestId,
        idxPropName       : realIdx,
        trackPropName     : strconv.FormatBool( azEvHub.trackTest ),
    }

    event := &evhub.Event {
        Properties   : appProps,
        PartitionKey : &id,
    }

    msg, err := azEvHub.msgGen.GetMsgN( azEvHub.MsgsPerSend, nil )
    if err != nil {
        glog.Errorf( "%v: Failed to get message, error = %v", id, err )
        return err
    }

    event.Data = msg

    err = azEvHub.hub.Send( azEvHub.senderCtx, event )
    if err != nil {
        glog.Errorf( "%v: Failed to send event, error = %v", id, err )
        return err
    }

    if azEvHub.trackTest {
        azEvHub.stats.UpdateSenderStat( realIdx, uint64( azEvHub.MsgsPerSend ) )
    }

    return nil
}

func ( azEvHub *AzEvHub )newSender( idx int )( err error ) {
    return nil
}

func ( azEvHub *AzEvHub )closeSender( idx int )( err error ) {
    return nil
}

func ( azEvHub *AzEvHub )startSender( idx int ) {
    err := azEvHub.newSender( idx )
    if err != nil {
        return
    }

    defer func( ) {
        azEvHub.closeSender( idx )
    }( )

    for {
        err = azEvHub.sendMessage( idx )
        if err != nil {
            return
        }

        select {
            case <-azEvHub.senderCtx.Done( ):
                break

            default:
        }

        time.Sleep( azEvHub.SendInterval )
    }

    return
}

func ( azEvHub *AzEvHub )getReceiverIdFromIdx( idx int )( id string, realIdx int, err error ) {
    realIdx = idx + ( azEvHub.Index * azEvHub.TotGateways )
    if realIdx >= len( azEvHub.idGen.Block ) {
        return "", 0, fmt.Errorf( "did not find id for index %v and offset index %v", idx, azEvHub.Index )
    }

    return azEvHub.idGen.Block[ realIdx ], realIdx, nil
}

func ( azEvHub *AzEvHub )receivedMessageCallback( idx int, event *evhub.Event )( err error ) {
    id, realIdx, err := azEvHub.getReceiverIdFromIdx( idx )
    if err != nil {
        glog.Errorf( "Failed to get index, error = %v", err )
        return err
    }

    trackVal, exists := event.Properties[ trackPropName ]
    if exists {
        track, ok := trackVal.( string )
        if ok && track == strFalse {
            return nil
        }
    }

    propVal, exists := event.Properties[ azEvHub.PropName ]
    if exists {
        sndid, ok := propVal.( string )
        if ok && id == sndid {
            return nil
        }
    }

    msgCb := func( msg *helpers.Msg )( err error ) {
        return azEvHub.msgGen.ValidateMsg( msg )
    }

    msgList, err := azEvHub.msgGen.ParseMsg( event.Data, msgCb )
    if err != nil {
        glog.Errorf( "%v: Failed to parse message, error = %v", id, err )
        return fmt.Errorf( "%v: Failed to parse message, error = %v", id, err )
    }

    if testIdPropVal, exists := event.Properties[ testIdPropName ]; exists {
        testId, ok := testIdPropVal.( string )
        if !ok || testId != azEvHub.TestId {
            glog.Errorf( "%v: Invalid test id in event properties", id )
            return fmt.Errorf( "%v: Invalid test id in event properties", id )
        }
    }

    if senderIdxPropVal, exists := event.Properties[ idxPropName ]; exists {
        senderIdx, ok := senderIdxPropVal.( int64 )
        if ok {
            azEvHub.stats.UpdateReceiverStat( realIdx, int( senderIdx ), uint64( msgList.Count ), uint64( msgList.GetLatency( ) ) )
        } else {
            glog.Errorf( "%v: Invalid sender index in event properties", id )
            return fmt.Errorf( "%v: Invalid sender index in event properties", id )
        }
    } else {
        glog.Errorf( "%v: Did not find sender index in event properties", id )
        return fmt.Errorf( "%v: Did not find sender index in event properties", id )
    }

    return nil
}

func ( azEvHub *AzEvHub )newReceiver( idx int )( err error ) {
    return nil
}

func ( azEvHub *AzEvHub )closeReceiver( idx int )( err error ) {
    return nil
}

func ( azEvHub *AzEvHub )getConsumerGroupForReceiver( idx int )( string ) {
    if len( azEvHub.consumerGroups[ idx ] ) == 0 {
        if len( azEvHub.ConsumerGroupPrefix ) > 0 {
            azEvHub.consumerGroups[ idx ] = azEvHub.ConsumerGroupPrefix + fmt.Sprint( idx )
        } else {
            azEvHub.consumerGroups[ idx ] = evhub.DefaultConsumerGroup
        }
    }


    return azEvHub.consumerGroups[ idx ]
}

func ( azEvHub *AzEvHub )startReceiver( idx int ) {
    err := azEvHub.newReceiver( idx )
    if err != nil {
        azEvHub.receiversChan[ idx ] <- err
        return
    }

    defer func( ) {
        azEvHub.closeReceiver( idx )
    }( )

    runtimeInfo, err := azEvHub.hub.GetRuntimeInformation( azEvHub.receiverCtx )
    if err != nil {
        azEvHub.receiversChan[ idx ] <- err
        return
    }

    consumerGroup := azEvHub.getConsumerGroupForReceiver( idx )

    cb := func( ctx context.Context, event *evhub.Event )( err error ) {
        return azEvHub.receivedMessageCallback( idx, event )
    }

    receiveOpts := [ ]evhub.ReceiveOption{ evhub.ReceiveWithConsumerGroup( consumerGroup ) }

    for _, partitionId := range runtimeInfo.PartitionIDs {
        checkPoint, err := azEvHub.Read( azEvHub.NameSpace, azEvHub.TopicName, consumerGroup, partitionId )

        if err != nil || checkPoint.Offset == evhub_persist.StartOfStream {
            receiveOpts = append( receiveOpts, evhub.ReceiveWithLatestOffset( ) )
        } else {
            receiveOpts = append( receiveOpts, evhub.ReceiveWithStartingOffset( checkPoint.Offset ) )
        }

        handle, err := azEvHub.hub.Receive( azEvHub.receiverCtx, partitionId, cb, receiveOpts... )
        if err != nil {
            azEvHub.receiversChan[ idx ] <- err
            return
        }

        defer func( ) {
            handle.Close( azEvHub.receiverCtx )
        }( )
    }

    azEvHub.receiversChan[ idx ] <- nil

    for {
        select {
            case <-azEvHub.receiverCtx.Done( ):
                return

            default:
        }
    }

    return
}
