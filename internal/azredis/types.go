package azredis

import (
    "time"
    "sync"
    "context"

    "github.com/go-redis/redis/v8"

    "github.com/azsvcbusbench/internal/helpers"
    "github.com/azsvcbusbench/internal/stats"
)

type azRedisCtx struct {
    client             *redis.Client

    senderCtx           context.Context
    receiverCtx         context.Context

    stats              *stats.Stats
    statsCtx            context.Context

    msgGen             *helpers.MsgGen
    idGen              *helpers.IdGen

    wg                 *sync.WaitGroup

    trackTest           bool
}

type AzRedis struct {
    TestId              string
    Host                string
    Password            string
    PropName            string

    IpsFile             string
    IdsFile             string

    TotSenders          int
    TotReceivers        int
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

    azRedisCtx
}
