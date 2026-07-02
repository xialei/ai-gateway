# AI Gateway Runtime 设计方案（最终版）

## 设计目标

打造一个**简单、高性能、轻量、可扩展**的大模型网关（AI Gateway
Runtime），聚焦自建 vLLM/SGLang 推理集群的高性能接入。

**核心差异化**：在推理集群的 KV Cache 之上做 Prefix-aware 亲和调度
（Consistent Hash + Instance Affinity），最大化实例级 KV 命中率、
降 TTFT、提 GPU 利用率——这是代理层（如 litellm）不存在的层，其
响应缓存替代不了（缓存输出 ≠ 提升推理引擎缓存利用率）。

**另一能力维度**：Context Pipeline 提供带延迟治理的深度上下文增强
（DAG 编排、超时预算、熔断、重计算异步预算），面向需要上下文工程
的场景。代价是比纯 proxy 更重，故不与 litellm 比生态广度与运营面。

聚焦：

-   高吞吐、低延迟
-   Context 优化（插件化）
-   Prefix Cache 调度
-   智能路由
-   外部模型脱敏
-   安全审计
-   多模型统一接入

## 核心设计原则

1.  **同步链路极简**：同步请求链路仅执行 O(1) 操作，不进行
    Embedding、Graph 查询、摘要生成等重计算。重计算（Summary、
    Ontology 构建、Memory 写入）经异步预计算，同步链路只读取最近一份
    缓存产物并应用（本地或一次 Redis GET，O(1) 级）。
2.  **插件化扩展**：Context、Ontology、Memory、RAG、PII 均通过 Pipeline
    Plugin 扩展。
3.  **调度与推理解耦**：Gateway 不维护 KV Cache，只进行 Prefix-aware
    Scheduler。
4.  **事件异步化**：审计、日志、Tracing、统计全部异步处理。

------------------------------------------------------------------------

# 总体架构

``` text
                 Client
                    │
        ┌───────────▼────────────┐
        │      AI Gateway        │
        │────────────────────────│
        │ Access                 │
        │ Request Normalize      │
        │ Context Pipeline       │
        │ Policy Engine          │
        │ Prefix Scheduler       │
        │ Model Connector        │
        └───────────┬────────────┘
                    │
      ┌─────────────┼──────────────┐
      │             │              │
   vLLM         SGLang        External LLM
```

## 六大核心模块

### 1. Access

-   TLS / Auth
-   Rate Limit
-   Streaming Proxy
-   HTTP2/gRPC 长连接

### 2. Context Pipeline（插件化）

插件示例： - Memory Plugin - Ontology Plugin - Summary Plugin - RAG
Plugin - Prompt Rewrite Plugin

说明： - Gateway 不直接实现业务逻辑。 - 所有 Context 能力通过插件扩展。

### 3. Policy Engine

负责： - Model Routing - Cost Policy - Latency Policy - ACL - PII
Decision

PII 检测仅决定： - 是否允许外发 - 是否需要脱敏

#### 控制面（精简）

不追求全面运营面，只暴露数据面已有决策的可观测与轻量管控：

-   **可观测**：Prefix 命中率、每实例负载/健康、Context 插件延迟与
    熔断状态、路由决策追溯（与 Async Event Bus 的 Audit/Trace 对齐）。
-   **轻量管控**：Virtual Key + 配额（对接 Cost Policy）、限流与熔断
    手动覆盖、路由策略切换。
-   **边界**：不做计费结算、不做模型 catalog 管理、不做完整 admin
    suite——这些非本网关目标，交上层平台。

### 4. Prefix Scheduler

职责： - Prefix Hash - Consistent Hash - Instance Affinity - Retry -
Failover

#### Instance Registry & State Sync

Consistent Hash + Instance Affinity 的前提是 Gateway 实时已知各推理实例的成员身份、健康状态与负载。否则哈希落点会指向已宕机或已打满的实例，导致 failover 频发、亲和性失效。prefix→instance 纯由环算出、不落库，故不违反“不维护 KV Cache”。

**多副本一致性**：Gateway 自身水平扩展，N 个副本若持有不同环视图，同一 prefix 在不同副本会路由到不同实例，缓存命中率归零。成员+健康状态须为共享视图，而非各副本本地状态。

1.  **服务发现（成员来源）**：复用部署环境原生能力（K8s Endpoints / Consul / etcd / Nacos），watch 流式订阅成员变更，不轮询；静态配置仅作冷启动兜底。
2.  **健康与负载信号（三类互补）**：
    - 主动探活：低频（1–5s）打 vLLM/SGLang 的 `/health`（liveness）。
    - 指标抓取：周期性拉 `/metrics`，取 `num_requests_running` / `num_requests_waiting` / `gpu_cache_usage_perc` / `num_preemption`，供亲和与过载判断。
    - 被动观测：请求路径上每实例延迟/错误率 → EWMA → 熔断器，最快感知恶化。
    - 主输入：Gateway 本地即持有每实例 in-flight 并发数，零成本且最鲜活，作为过载判断主输入，`/metrics` 的 KV 占用与队列深度作补充。
3.  **状态同步模型**：成员+健康快照以 Redis（或 registry 本身）为单一真相源；各副本持有本地 ring 副本，经 pub/sub 或 watch 增量同步；成员变更触发虚拟节点环重建。重建须廉价且低频——环抖动会系统性打掉亲和性，故需抖动抑制（实例上下线去抖、最小稳定窗口）。
4.  **故障与亲和性取舍**：
    - 不健康实例从环摘除（或权重置 0）→ 环重建 → 新请求绕开，在途请求不丢。
    - 请求中实例挂了 → failover 到环上下一节点 → 必然丢本次亲和性，接受（本非缓存保证）。
    - KV 被实例 LRU 驱逐 → Gateway 不感知，仍按哈希路由 → miss；可加可选反馈环：实例在响应头回报 hit/miss 作为弱 hint 决定续粘或换节点，不建 cache map，不违反原则。

### 5. Model Connector

统一 OpenAI Compatible Connector： - vLLM - SGLang - OpenAI -
Anthropic - MCP/Tool

### 6. Async Event Bus

全部异步： - Audit - Metrics - OpenTelemetry Trace - Logging - Usage
Statistics

------------------------------------------------------------------------

# 请求流程

1.  Access
2.  Request Normalize
3.  Context Pipeline（插件执行）
4.  Policy Engine
5.  Prefix Scheduler
6.  Model Connector
7.  Streaming Response
8.  Async Audit/Event

------------------------------------------------------------------------

# Context Pipeline

采用责任链：

Memory → Ontology → Summary → RAG → Prompt Rewrite

新增能力仅新增插件，不修改 Gateway Core。

## 执行类别与延迟治理

Context 插件是 P99 延迟的主要来源之一，按重计算是否进入同步链路
切为两类：

-   **Async / 预计算类（重）**：Summary、Ontology 构建、Memory 写入。
    计算发生在请求路径之外（对话更新时、定时、写回时），同步链路只读
    取最近一份缓存产物并应用。失败 → 用 last-good 或空，绝不阻塞。
-   **Sync / 即时类（轻或不可省）**：RAG 检索（当前 query 的检索有界、
    无法完全预算）、Prompt Rewrite（轻变换）。真在路径上，但必须有界。

### 超时预算与传播

-   **预算来源挂 Policy Engine**：按请求延迟类（strict / normal / loose）
    映射链路预算，而非每插件各拍一个。
-   **共享且递减的令牌**：预算穿过整条链，每插件取
    `min(自身上限, 剩余预算)`，防早期慢插件吃光预算而尾部插件仍在傻等。
-   同步路径上**不重试**——重试是尾部延迟的乘法器，重试留给 async 侧。

### 失败与 Fallback

-   默认 **fail-open**：Context 插件是上下文增强，缺失增强不应让用户
    请求失败。（PII/ACL 在 Policy Engine，本就 fail-closed，不在此链。）
-   **last-good**：插件有最近缓存产物则用缓存，而非直接空。
-   **显式降级**：RAG 超时 → 返回更少/零 chunk；Summary 不可用 → 省略；
    Rewrite 失败 → 原文透传。降级是显式的，不是异常冒泡。

### 熔断器

-   每插件 + 每下游依赖一个熔断器。某外部 summary/embedding 服务近期
    错误/超时率高 → 触发 → 一段时间内直接走 fallback，而非每请求都付
    满超时。下游宕机时此条比任何 timeout 都管用。
-   熔断状态**跨 Gateway 副本共享**（走 Instance Registry / Redis），
    所有副本一起退避，否则单副本各自重试会持续打挂下游。

### 执行模型：DAG 而非线性链

依赖未必线性——Summary 不必等 Ontology，RAG 检索与 Memory 读可能相互
独立。建成 **DAG**，独立分支在共享预算下并行，尾部延迟 = 最慢路径
而非全链求和，不增加复杂度原则即获 P99 收益。

### 插件接口契约

“插件化扩展”的可执行性取决于接口，定三条契约：

-   **依赖声明（DAG 边来源）**：插件注册时声明 `produces` / `consumes`
    的具名产物（如 Memory produces `mem_context`，RAG consumes 之并
    produces `rag_chunks`，Rewrite consumes 两者）。Gateway 启动期据此
    构图，检测环、并行调度独立分支。边是声明数据，不是代码调用——
    插件互不引用，顺序由声明决定而非写死在链里，方保“不修改 Core”。
-   **熔断状态上报**：度量在网关侧（每插件 ID 的延迟/错误率 → EWMA
    → 状态写共享 Redis，跨副本一致，复用 Instance Registry 的同步层）；
    插件**只读**自身熔断状态决定是否短路走 fallback。度量归网关、
    短路决策归插件、状态存储归共享层，插件不背状态管理复杂度。
-   **超时预算传入**：插件接口签名带 `deadline` / `remaining_budget`
    入参（令牌递减模型）。插件据此决定截断检索量、跳过可选分支、或
    直接返回缓存。预算是入参不是插件自定，呼应“预算挂 Policy Engine、
    共享递减”。

------------------------------------------------------------------------

# Prefix Cache

目标： - 最大化 KV Cache 命中率 - 降低 TTFT - 提高 GPU 利用率

边界声明： Consistent Hash + Instance Affinity 是命中率的启发式
提升，不是缓存保证。KV 被实例 LRU 驱逐、环抖动、多副本短暂分歧
都会拉低命中，故不能简单视为“有缓存”。实际命中与驱逐由推理实例
自治，Gateway 仅保证路由层面的亲和倾向。

------------------------------------------------------------------------

# 外部模型脱敏

流程：

Detect → Replace → Forward → Restore

特点： - Redis 保存映射（短 TTL） - 请求结束立即删除映射

## 流式恢复机制

“流式响应实时恢复”是全设计技术难度最高点之一：token chunk 边界
≠ 实体边界 ≠ 替换符边界，三者不对齐，占位符会被任意切在 chunk 间。

-   **替换符**：确定性格式带清晰起止哨兵（如 `\x00PH:<id>\x00`），
    哨兵序列不出现在正常 token 流中。Redis 存 `id → 原文实体`。
-   **跨 chunk 缓冲**：per-stream 状态机缓冲区。仅当缓冲含完整哨兵对
    （或确认不可能是哨兵前缀）才 flush 给客户端，不完整则攒等下一 chunk。
    非字符串替换，是状态机。缓冲必须有界，超阈值强制 flush 牺牲本次
    恢复，防异常流卡死内存。

三档 fallback（轻 → 重）：

1.  哨兵被跨 chunk 切：缓冲重组，对客户端透明——默认路径，非错误。
2.  Redis TTL 到期但流未结束（短 TTL 的直接后果）：查不到映射 → 该
    占位符以 `[REDACTED]` 字面透传，流不中断，同步记 audit event。
    绝不阻塞或重试。
3.  流错乱 / 缓冲溢出：终止恢复，已恢复保留，未恢复降级 `[REDACTED]`，
    事件落审计。

边界声明：流式恢复尽力而为，保证流不中断与请求方可见性，不保证
占位符 100% 还原。TTL 窗口外的占位符降级为 `[REDACTED]`，是用短
TTL 换内存安全的有意取舍。

------------------------------------------------------------------------

# 安全审计

同步链路仅发送 Event：

Gateway → Event Bus → Kafka → ClickHouse/Object Storage

记录： - Route Decision - PII Result - Token Usage - Latency - Request
ID

------------------------------------------------------------------------

# 技术选型

-   Go（推荐）
-   HTTP/2 + gRPC
-   Redis
-   Kafka
-   OpenTelemetry
-   ClickHouse
-   vLLM / SGLang

------------------------------------------------------------------------

# 语言与加速边界

网关主体为 Go，goroutine 并行、无 GIL，Prompt Normalize、Prefix Hash
等 O(n)/O(1) 延迟敏感路径保持纯 Go，不引入 Rust/CGO 或 Sidecar。
litellm 的 Rust 加速收益源于 Python GIL，对 Go 不适用；其普适经验
（JSON 边界一次性过、热连接池预热握手）Go 自身即可实现。

判据（留给将来真出现重计算路径时）：

-   **FFI（CGO/Rust）**：适用纯函数、无 I/O、调用极高频、状态留在
    Go 侧。代价：CGO 调用有固定开销（非零）、进 CGO 期间 goroutine
    绑定 OS 线程且调度器失去抢占权、跨语言 GC/指针边界易错、构建链
    复杂。仅当 Rust 侧省下的时间显著大于边界开销时才值。
-   **Sidecar**：适用有状态、需独立扩缩容的解耦部署。代价：每次调用
    一次网络往返、多一进程运维、故障域扩大，与延迟敏感同步路径互斥。

一句话：延迟敏感 + 纯函数 → 留 Go；延迟敏感 + 有状态重计算 → 重新
审视该路径是否该在同步链路，而非套 Sidecar。

------------------------------------------------------------------------

# 非目标

Gateway 不负责： - Embedding - Graph Query - Agent Runtime - Workflow -
Memory Storage - Knowledge Graph 推理

这些能力全部通过插件或上层 Agent 提供。

------------------------------------------------------------------------

# 总结

六大核心模块（Access / Context Pipeline / Policy Engine / Prefix
Scheduler / Model Connector / Async Event Bus）围绕同步链路极简、
插件化扩展、调度与推理解耦、事件异步化四原则组织，兼顾高性能、
轻量与长期可扩展性。
