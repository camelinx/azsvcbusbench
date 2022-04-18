package azsvcbusbench

import (
    "time"
    "sync"
    "context"

    "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

type azSvcBusBenchCtx struct {
    client             *azservicebus.Client
    ctx                 context.Context
    wg                 *sync.WaitGroup
}

type AzSvcBusBench struct {
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

    azSvcBusBenchCtx
}
