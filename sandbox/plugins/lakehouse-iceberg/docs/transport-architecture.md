# OpenSearch Transport Architecture

## Transport Layer Overview

```
                    TransportService.sendRequest(node, action, request)
                                        │
                                        ▼
                              ConnectionManager
                              ┌─────────────────────┐
                              │ connectedNodes map   │
                              │ nodeB → Connection   │
                              │   ├── LIGHT channels │
                              │   ├── REG channels   │
                              │   └── HEAVY channels │
                              └────────┬────────────┘
                                       │
                          picks channel by request type
                                       │
                    ┌──────────────────┴──────────────────┐
                    │                                      │
            Netty4 Transport                       Flight Transport
            ────────────────                       ────────────────
                    │                                      │
                    ▼                                      ▼
          Netty4TcpChannel                        FlightTcpChannel
                    │                                      │
                    ▼                                      ▼
           OutboundHandler                       FlightOutboundHandler
           ┌──────────────┐                      ┌────────────────────┐
           │ StreamOutput  │                      │ VectorStreamOutput │
           │ → bytes       │                      │ → Arrow buffers    │
           │ + TcpHeader   │                      │ + Flight Ticket    │
           └──────┬───────┘                      └────────┬───────────┘
                  │                                        │
                  ▼                                        ▼
           Netty Pipeline                            gRPC Stream
           ┌──────────────┐                      ┌────────────────────┐
           │ Custom binary │                      │ putNext(root)      │
           │ frame over    │                      │ over HTTP/2        │
           │ TCP           │                      │ over TCP           │
           └──────┬───────┘                      └────────┬───────────┘
                  │                                        │
                  └──────────────┬──────────────────────────┘
                                 │
                                 ▼
                          ════════════
                            NETWORK
                          ════════════
                                 │
                    ┌────────────┴────────────────┐
                    │                              │
                    ▼                              ▼
           Netty4 Inbound                  FlightProducer
           ┌──────────────┐                ┌────────────────────┐
           │ InboundPipe-  │                │ Receives gRPC call │
           │ line decodes  │                │ Creates             │
           │ byte frames   │                │ FlightServerChannel │
           │ → StreamInput │                │ → VectorStreamInput │
           └──────┬───────┘                └────────┬───────────┘
                  │                                  │
                  └──────────────┬───────────────────┘
                                 │
                                 ▼
                    InboundHandler / NativeMessageHandler
                                 │
                                 ▼
                    RequestHandlerRegistry
                    → dispatches to TransportAction handler
```

## Native Arrow PR Changes (PR #21240 vs Rishabh's #21253)

The changes are all on the **Flight side only** — Netty4 and core transport are untouched.

```
                    TransportService.sendRequest(node, action, request)
                                        │
                                        ▼
                              ConnectionManager
                              ┌─────────────────────┐
                              │  (no changes here)   │
                              └────────┬────────────┘
                                       │
                    ┌──────────────────┴──────────────────┐
                    │                                      │
            Netty4 Transport                       Flight Transport
            (no changes)                           (ALL CHANGES HERE)
                                                           │
                                                           ▼

    ★ SEND SIDE (server responding with Arrow data)
    ═══════════════════════════════════════════════

    TransportAction handler has a VectorSchemaRoot (e.g. from DataFusion)
                    │
                    ▼
    ┌───────────────────────────────────────────────────────────────┐
    │ PR #21240 (current)              │ PR #21253 (Rishabh)        │
    │──────────────────────────────────│────────────────────────────│
    │                                  │                            │
    │ Producer uses OWN allocator      │ Producer gets allocator    │
    │ wraps root in ArrowBatchResponse │ from ArrowFlightChannel    │
    │          │                       │ .from(channel)             │
    │          ▼                       │ .getAllocator()            │
    │ FlightTransportChannel           │          │                 │
    │ .sendResponseBatch()             │          ▼                 │
    │          │                       │ Creates root from Flight's │
    │          ▼                       │ allocator, transfers data  │
    │ FlightOutboundHandler            │ via TransferPair.transfer()│
    │ ┌─────────────────────┐          │          │                 │
    │ │ instanceof check:   │          │          ▼                 │
    │ │ ArrowBatchResponse? │          │ wraps in ArrowBatchResponse│
    │ │  YES → sendArrowBatch│         │ (writeTo() is final no-op)│
    │ │  NO  → byte path    │          │          │                 │
    │ └─────────┬───────────┘          │          ▼                 │
    │           │                      │ FlightOutboundHandler      │
    │           ▼                      │ transfer() into sharedRoot │
    │ FlightServerChannel              │          │                 │
    │ externalRoot = true              │          ▼                 │
    │ putNext(caller's root)           │ putNext(sharedRoot)        │
    │                                  │ (channel owns everything)  │
    │ ⚠️ LEAK: nobody closes root     │ ✅ channel closes old root │
    │ ⚠️ Core changes: getDelegate()  │ ✅ no core changes         │
    │ ⚠️ No pipelining                │ ✅ pipelining safe         │
    └───────────────────────────────────────────────────────────────┘
                    │
                    ▼
              ════════════
                NETWORK
              ════════════
                    │
                    ▼

    ★ RECEIVE SIDE (client consuming Arrow data)
    ═════════════════════════════════════════════

    ┌───────────────────────────────────────────────────────────────┐
    │ PR #21240 (current)              │ PR #21253 (Rishabh)        │
    │──────────────────────────────────│────────────────────────────│
    │                                  │                            │
    │ FlightTransportResponse          │ FlightTransportResponse    │
    │          │                       │          │                 │
    │ walks getDelegate() chain        │ creates VectorStreamInput  │
    │ to find ArrowStreamHandler       │          │                 │
    │          │                       │          ▼                 │
    │ instanceof ArrowStreamHandler?   │ Handler calls              │
    │  YES → pass root directly        │ ((VectorStreamInput) in)   │
    │  NO  → byte path                 │ .getRoot()                 │
    │                                  │                            │
    │ ⚠️ Core changes: getDelegate()  │ ✅ no branching            │
    │ ⚠️ Decorator chain walking      │ ✅ no core changes         │
    └───────────────────────────────────────────────────────────────┘
                    │
                    ▼
    Consumer gets VectorSchemaRoot
    (process inline or copy to own allocator)
```
