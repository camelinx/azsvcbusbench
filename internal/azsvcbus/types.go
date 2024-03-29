package azsvcbus

import (
    "time"
    "sync"
    "context"

    "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
    "github.com/azsvcbusbench/internal/helpers"
    "github.com/azsvcbusbench/internal/stats"
)

type azSvcBusCtx struct {
    client             *azservicebus.Client
    senders         [ ]*azservicebus.Sender
    receivers       [ ]*azservicebus.Receiver

    senderCtx           context.Context
    receiverCtx         context.Context

    stats              *stats.Stats
    statsCtx            context.Context

    msgGen             *helpers.MsgGen
    idGen              *helpers.IdGen

    wg                 *sync.WaitGroup

    trackTest           bool
}

type AzSvcBus struct {
    TestId              string
    ConnStr             string
    TopicName           string
    SubName             string
    PropName            string

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

    azSvcBusCtx
}
