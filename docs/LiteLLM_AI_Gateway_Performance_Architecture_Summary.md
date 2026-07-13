# LiteLLM 性能提升架构选择总结

## ------ 面向企业 AI Gateway 设计的参考

## 一句话总结

LiteLLM 的性能演进并不是单纯优化代码，而是在构建一个
**薄（Thin）、快（Fast）、可治理（Governable）的 AI Gateway Runtime**。

它通过不断压缩高频请求路径（HTTP、Streaming、Connection、Serialization）的开销，将性能敏感部分逐步向更高性能运行时演进，同时把策略、权限、Provider
管理、MCP 等低频控制逻辑保留在控制面，最终形成类似：

    Control Plane + High Performance Data Plane

的架构。

核心目标：

> Gateway 自身延迟和 CPU
> 消耗接近不可感知，把资源留给真正的推理引擎（vLLM、SGLang、TensorRT-LLM）。

------------------------------------------------------------------------

# LiteLLM 性能架构六大原则

## 1. Gateway 永远不要成为瓶颈（Thin Runtime）

LiteLLM 的核心选择：

> 最常见请求必须走最短路径。

典型链路：

    Client
     |
    HTTP
     |
    Authentication
     |
    Router
     |
    Provider
     |
    Streaming
     |
    Client

持续优化：

-   Chat Completion Fast Path
-   Responses Fast Path
-   Streaming Fast Path

减少：

-   不必要 Middleware
-   参数转换
-   对象创建
-   JSON Copy
-   Validation

设计原则：

> Hot Path 越短，Gateway 越接近零开销。

------------------------------------------------------------------------

# 2. Hot Path 与 Cold Path 分离

这是 AI Gateway 最重要的架构原则。

## Hot Path

高频实时路径：

    HTTP
    Streaming
    Connection
    Serialization
    Cancellation
    Routing

要求：

-   无数据库访问
-   无复杂计算
-   无阻塞操作
-   尽量 Zero Copy

------------------------------------------------------------------------

## Cold Path

低频管理路径：

    Dashboard
    Budget
    Admin
    OAuth
    MCP Management
    Metrics
    Audit

允许：

-   数据库
-   复杂策略
-   异步任务

------------------------------------------------------------------------

# 3. Streaming 优先于普通 HTTP

LLM Gateway 与传统 API Gateway 最大区别：

普通 API：

    Request
     |
    Response

LLM：

    Request
     |
    Chunk
     |
    Chunk
     |
    Chunk
     |
    Complete

因此 Streaming 是核心性能路径。

重点优化：

-   SSE
-   Chunk 管理
-   Buffer 复用
-   Flush 策略
-   Backpressure
-   Client Disconnect Cancel

目标：

-   更低 TTFT
-   更稳定 P99
-   不丢 Token
-   更低 CPU

------------------------------------------------------------------------

# 4. 减少对象复制，而不是简单减少代码

AI Gateway 最大 CPU 消耗之一：

    Object
     |
    Object
     |
    Object
     |
    JSON
     |
    Provider

LiteLLM 的优化方向：

-   Fast Path
-   Skip Validation
-   Provider Native Support
-   Typed Serialization

核心思想：

> 每一次对象创建、转换、序列化，都会增加延迟。

对于 Go/Rust：

应该进一步做到：

-   Zero Copy
-   Buffer Reuse
-   Native Serialization

------------------------------------------------------------------------

# 5. Connection 比 Request 更重要

随着：

-   Agent
-   Streaming
-   Voice
-   Realtime

发展，请求生命周期越来越长。

因此 Gateway 必须优化：

    Connection Pool
    Keep Alive
    Timeout
    Cancellation
    Reuse

核心思想：

> AI Gateway 的资源单位正在从 Request 转变为 Connection。

------------------------------------------------------------------------

# 6. Rust 只负责 Runtime，而不是全部重写

LiteLLM 的演进方向不是完全 Rust 化。

更合理架构：

    Python
     |
     + Provider Adapter
     + Plugin
     + Policy
     + Management


    Rust
     |
     + HTTP Runtime
     + Streaming
     + Connection
     + Parser
     + Serialization

原则：

-   业务逻辑需要灵活性
-   数据面需要极致性能

------------------------------------------------------------------------

# LiteLLM 性能架构总结图

                  AI Gateway Runtime

            ┌──────────────────────┐
            │      Client          │
            └──────────┬───────────┘
                       |
                  HTTP Runtime
                       |
              Authentication
                       |
                 Model Router
                       |
              Cache Fast Path
                       |
              Provider Adapter
                       |
              Streaming Runtime
                       |
              Metrics / Audit

目标：

    Gateway Overhead → 接近 0

------------------------------------------------------------------------

# 对下一代企业 AI Gateway 的启发

LiteLLM 已经证明：

高性能 Gateway 必须具备：

1.  Thin Runtime
2.  Hot Path 优化
3.  Streaming First
4.  Connection Oriented
5.  Control Plane / Data Plane 分离
6.  Runtime 高性能化

------------------------------------------------------------------------

# 超越 LiteLLM：Inference-aware Gateway

LiteLLM 当前主要解决：

    HTTP Gateway
            |
    Provider
            |
    LLM

下一代企业 AI Gateway 应进一步理解推理系统：

    HTTP
     |
    Auth
     |
    Model Router
     |
    Prefix Cache Router
     |
    Context Cache Router
     |
    PD Router
     |
    KV Cache Router
     |
    GPU-aware Scheduler
     |
    vLLM / SGLang

核心区别：

LiteLLM：

> 优化 Gateway Runtime

下一代 AI Gateway：

> 优化整个 Inference Runtime

------------------------------------------------------------------------

# 最终架构判断

未来企业 AI Gateway 应该成为：

                     AI Gateway

            Control Plane
     ┌────────────────────────┐
     │ Auth                   │
     │ Policy                 │
     │ Budget                 │
     │ MCP                    │
     │ Audit                  │
     └────────────────────────┘


            Data Plane
     ┌────────────────────────┐
     │ HTTP Runtime           │
     │ Streaming Runtime      │
     │ Routing Runtime        │
     │ Cache Runtime          │
     │ Inference Router       │
     └────────────────────────┘


                  |
          vLLM / SGLang / TRT-LLM

核心目标：

> 用最低 Gateway 开销，实现最高 AI 推理利用率。
