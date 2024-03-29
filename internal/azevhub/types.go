package azevhub

import (
    "time"
    "sync"
    "context"

    evhub "github.com/Azure/azure-event-hubs-go/v3"
    evhub_persist "github.com/Azure/azure-event-hubs-go/v3/persist"

    "github.com/azsvcbusbench/internal/helpers"
    "github.com/azsvcbusbench/internal/stats"
)

type azEvHubCtx struct {
    hub                *evhub.Hub

    persister           evhub_persist.CheckpointPersister

    senderCtx           context.Context
    receiverCtx         context.Context

    receiversChan    [ ]chan error
    consumerGroups   [ ]string

    stats              *stats.Stats
    statsCtx            context.Context

    msgGen             *helpers.MsgGen
    idGen              *helpers.IdGen

    wg                 *sync.WaitGroup

    trackTest           bool
}

type AzEvHub struct {
    TestId              string
    ConnStr             string
    NameSpace           string
    TopicName           string
    ConsumerGroupPrefix string
    PropName            string

    PersistDir          string

    IpsFile             string
    IdsFile             string

    TotGateways         int
    MsgsPerReceive      int
    MsgsPerSend         int

    SenderOnly          bool
    ReceiverOnly        bool

    WarmupDuration      time.Duration
    Duration            time.Duration
    SendInterval        time.Duration
    ReceiveInterval     time.Duration
    StatDumpInterval    time.Duration

    Index               int

    azEvHubCtx
}
