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

    senderCtx           context.Context
    receiverCtx         context.Context

    stats              *stats.Stats
    statsCtx            context.Context

    msgGen             *helpers.MsgGen
    idGen              *helpers.IdGen

    wg                 *sync.WaitGroup
}

type AzSvcBus struct {
    ConnStr             string
    TopicName           string
    SubName             string
    PropName            string

    IpsFile             string
    IdsFile             string

    TotSenders          int
    TotReceivers        int

    SenderOnly          bool
    ReceiverOnly        bool

    Duration            time.Duration
    SendInterval        time.Duration
    ReceiveInterval     time.Duration
    StatDumpInterval    time.Duration

    azSvcBusCtx
}
