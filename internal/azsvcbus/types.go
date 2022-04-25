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

    uuids            [ ]string

    stats              *stats.Stats
    statsCtx            context.Context

    msgs               *helpers.Msgs

    wg                 *sync.WaitGroup
}

type AzSvcBus struct {
    ConnStr             string
    TopicName           string
    SubName             string
    PropName            string

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
