# AI Gateway Runtime 设计方案（最终版）

## 设计目标

打造一个**简单、高性能、轻量、可扩展**的大模型网关（AI Gateway
Runtime），聚焦：

-   高吞吐、低延迟
-   Context 优化（插件化）
-   Prefix Cache 调度
-   智能路由
-   外部模型脱敏
-   安全审计
-   多模型统一接入

## 核心设计原则

1.  **同步链路极简**：同步请求链路仅执行 O(1) 操作，不进行
    Embedding、Graph 查询、摘要生成等重计算。
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

### 4. Prefix Scheduler

职责： - Prefix Hash - Consistent Hash - Instance Affinity - Retry -
Failover

注意： Gateway 不维护 KV Cache，只负责将相同 Prefix 调度到同一推理实例。

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

------------------------------------------------------------------------

# Prefix Cache

目标： - 最大化 KV Cache 命中率 - 降低 TTFT - 提高 GPU 利用率

策略： - Prompt Normalize - Prefix Hash - Consistent Hash - Instance
Affinity

------------------------------------------------------------------------

# 外部模型脱敏

流程：

Detect → Replace → Forward → Restore

特点： - Redis 保存映射（短 TTL） - 流式响应实时恢复 -
请求结束立即删除映射

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

# 非目标

Gateway 不负责： - Embedding - Graph Query - Agent Runtime - Workflow -
Memory Storage - Knowledge Graph 推理

这些能力全部通过插件或上层 Agent 提供。

------------------------------------------------------------------------

# 总结

最终 AI Gateway Runtime 保持只有六个核心模块：

1.  Access
2.  Context Pipeline
3.  Policy Engine
4.  Prefix Scheduler
5.  Model Connector
6.  Async Event Bus

整体架构保持同步链路极简、插件化扩展、调度与推理解耦、事件异步化，兼顾高性能、轻量和长期可扩展性。
