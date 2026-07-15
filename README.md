# ai-gateway

一个**简单、高性能、轻量、可扩展**的 AI 网关运行时（AI Gateway Runtime），面向自建
vLLM / SGLang 推理集群。使用 Go 编写。

核心差异化能力是在推理集群的 KV Cache 之上做 **Inference-aware 调度**：在 prefix 亲和
（一致性哈希 + 实例亲和）的基础上，叠加**实时负载感知**（从后端 `/metrics` 抓取的 GPU
缓存占用 / 等待队列 / 并发数）与 **KV 命中反馈闭环**（解析后端流式 usage 中的
`cached_tokens`，回写每实例命中率 EWMA，长期低命中的亲和目标被自动降权）——把“猜的
亲和”变成“验证过的亲和”，这是代理层（如 litellm）不存在的层。

另一能力维度是 **Context Pipeline**：带延迟治理的深度上下文增强（DAG 编排、超时预算、
熔断、重计算异步预算），以及面向外部模型外发的**流式 PII 脱敏 / 恢复**。

完整设计见 [`docs/AI_Gateway_Runtime.md`](docs/AI_Gateway_Runtime.md)；性能架构对照与
优化记录见 [`docs/LiteLLM_AI_Gateway_Performance_Architecture_Summary.md`](docs/LiteLLM_AI_Gateway_Performance_Architecture_Summary.md)。

## 当前状态

端到端数据面已实现，所有外部依赖（Redis / Kafka / ClickHouse / 服务发现）均以接口 +
内存默认实现提供，真实适配器为后续工作。开箱即可对内置 mock 后端运行——`make demo`
无需任何外部服务。默认事件 sink 为 no-op，演示运行不产生逐请求日志。

## 快速开始

### 1. 编译

```bash
make build       # 产出 bin/ai-gateway
# 或直接 go build -o bin/ai-gateway ./cmd/gateway
```

要求 Go 1.25+。

### 2. 跑起来（内置 mock，零外部依赖）

```bash
make demo        # 启动 + 自动发一个示例请求 + 退出
```

或后台手动跑：

```bash
go run ./cmd/gateway -config config.demo.yaml
```

`config.demo.yaml` 里 `scheduler.mock: true` 会拉起一个进程内 mock 后端，开箱即可
跑通整条链路（鉴权 → 调度 → 转发 → 流式 → PII 恢复 → 事件）。**注意：mock 仅供
demo / 本地开发，生产不要开**——`instances` 为空且 `mock: false` 时，网关以"无后端"
启动，请求会 503 直到服务发现注入成员。

### 3. 发请求

网关对外是标准 OpenAI Chat Completions 协议，客户端无需改动：

```bash
curl -N -H "Authorization: Bearer sk-gateway-demo" \
     -H "Content-Type: application/json" \
     -d '{"model":"demo-model","stream":true,"messages":[{"role":"user","content":"hi"}]}' \
     http://127.0.0.1:8080/v1/chat/completions
```

- `Authorization: Bearer <key>` —— 在 `access.api_keys` 里登记的 key；留空则任意 key
  放行（按 key 独立限流）。
- `model` —— 必须在 `policy.routes` 里有映射，否则按"模型名即路由目标"处理。
- `stream: true/false` —— 流式与非流式都支持。

健康检查：

```bash
curl http://127.0.0.1:8080/healthz   # -> ok
```

### 4. 接真实后端（vLLM / SGLang / 任意 OpenAI 兼容）

编辑 `config.example.yaml`，把 `instances` 指向你的端点：

```yaml
instances:
  - id: vllm-0
    base_url: http://10.0.0.1:8000
    model: qwen2.5-72b
    weight: 1
  - id: vllm-1
    base_url: http://10.0.0.2:8000
    model: qwen2.5-72b
    weight: 1
```

```bash
make run CONFIG=config.example.yaml
# 或 ./bin/ai-gateway -config config.example.yaml
```

多实例时自动启用 **prefix 亲和 + 负载感知调度**：相同前缀（system + 历史）的请求
优先打到最可能已缓存 KV 的实例，并在候选中按实时负载选最优；连接失败自动 failover
到环上下一节点。确保后端的 `/metrics`（vLLM/SGLang Prometheus 端点）可达，调度器会
定期抓取 `num_requests_running` / `num_requests_waiting` / `gpu_cache_usage_perc`
作为负载信号。

### 5. 外部模型 + PII 脱敏

发往外部模型（OpenAI / Anthropic 等）的请求可自动脱敏，避免 PII 外泄。在 `policy`
里把目标标为 `external_targets`：

```yaml
policy:
  routes:
    "gpt-4o": openai-external
  external_targets:
    - openai-external
pii:
  enabled: true
  patterns:
    - name: email
      pattern: '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}'
    - name: phone
      pattern: '\b1[3-9]\d{9}\b'
```

请求外发前，命中的 PII 会被替换为哨兵占位符并建立映射；流式回包时再用有界状态机
还原。**安全行为**：若某目标被标为需脱敏但 PII 未启用（或规则编译失败），网关会
**拒绝转发（503）**而非泄露原文——fail-closed。

### 6. 优雅停机

`SIGINT` / `SIGTERM` 触发：停止健康探测与事件总线、`http.Server.Shutdown`（5s 超时）、
排空事件缓冲、关闭 mock 监听器（若启用）。


## 模块

| 模块 | 包 | 职责 |
|---|---|---|
| Access | `internal/access` | API Key 鉴权、按 Key 令牌桶限流 |
| Normalize | `internal/normalize` | OpenAI 兼容请求 → 内部 `Request`（容忍未知字段） |
| Context Pipeline | `internal/context` | 插件 DAG、共享预算、fail-open、每插件熔断 |
| Policy Engine | `internal/policy` | 路由、延迟类预算、ACL、PII 决策 |
| Prefix Scheduler | `internal/scheduler` | 一致性哈希 + 虚拟节点、prefix 亲和、**负载感知**、注册表、failover |
| Model Connector | `internal/connector` | OpenAI 兼容流式转发 + SSE 透传、连接池调优、HTTP/2 |
| PII | `internal/pii` | Detect → Replace（哨兵）→ Restore（有界状态机） |
| Async Event Bus | `internal/eventbus` | 审计 / 指标 / 链路 / 用量，全部异步、默认 no-op sink |

共享工具位于 `pkg/`（`ewma`、`sse`）。

## 请求流程

1. **Access** — 鉴权 + 限流
2. **Normalize** — OpenAI 负载 → `Request`
3. **Policy Engine** — 路由目标、延迟预算、ACL、PII 决策
4. **Context Pipeline** — 插件 DAG（fail-open，预算递减）
5. **PII** — 外发前脱敏请求（若决策为 redact）
6. **Prefix Scheduler** — prefix 亲和候选 + 实时负载选实例，连接失败则 failover
7. **Model Connector** — 转发 + 流式透传
8. **PII** — 流式恢复状态机（若已脱敏）
9. **KV 命中反馈** — 零阻塞透传 tap 解析后端 usage，回写实例命中率 EWMA
10. **Async Event Bus** — 路由决策 / PII 结果 / 延迟 / 用量

同步链路只做 O(1) / 流式透传工作；重计算由异步预计算完成，同步链路只读取缓存产物。
事件 emit 在 bus 未启用时零成本短路（无池获取、无 channel）。

## Inference-aware 调度

调度器不是“把请求路由到某个实例”，而是“在 KV Cache 之上做最优放置”：

- **Prefix 亲和候选**：以请求前缀（system + 历史，不含末轮用户输入）为一致性哈希键，
  取环上最近 N 个实例作为候选——它们最可能已缓存该 prompt 的 KV。
- **实时负载打分**：在候选中按 `InFlight`（主过载信号）+ `WaitingRequests`（达阈值则重罚，
  其 KV 正在被驱逐）+ `GPUCacheUsage`（缓存越满越没余量）打分，选最优。
- **亲和不退化**：均衡负载下所有候选同分，首候选（纯亲和宿主）胜出 → KV 命中率不退化。
  `load_aware_candidates: 1` 即退化为纯 prefix 亲和。
- **KV 命中反馈闭环**：流式 tap 从后端 usage chunk 解析 `cached_tokens / prompt_tokens`，
  回写每实例 `cacheHitRate` EWMA；长期低命中的亲和目标在打分中被 discount。

相关配置（`scheduler` 段）：`load_aware_candidates`、`waiting_requests_threshold`。

## 配置

完整示例见 [`config.example.yaml`](config.example.yaml)（接真实后端）、
[`config.demo.yaml`](config.demo.yaml)（内置 mock）。命令行只有一个参数：

```bash
./bin/ai-gateway -config <path-to-yaml>
```

关键段：

- `instances` — 后端成员（静态；动态发现是 `scheduler.InstanceSource` 背后的适配器）。
  为空且 `scheduler.mock: false` 时网关以"无后端"启动，请求 503 直到发现机制注入成员。
- `scheduler.mock` — **demo / 本地开发专用**。`true` 时拉起进程内 mock 后端；生产环境
  保持 `false`（默认）。空实例 + `mock: true` 才会服务 mock 响应，避免生产环境误配发假数据。
- `latency` — 按延迟类的链路预算（`strict` 500ms / `normal` 2s / `loose` 5s）。预算同时
  约束 Context Pipeline 与后端"连接 + 首字节"耗时，流式 tail 不受限。
- `access` — `api_keys` 白名单（留空则任意 key 放行）；`rate_limit_per_second` /
  `rate_limit_burst` 按 key 独立限流。**未设置时默认 100/s、burst 200**（不会因零值
  把流量卡死）。
- `policy` — 模型→路由映射、ACL（api key → 允许的模型，留空 = 全部）、`external_targets`
  （需 PII 脱敏的外部目标）。
- `context` — 插件 DAG 开关（内置无插件；Memory / RAG / Rewrite / Summary 由上层实现并
  注册到 `buildContextPlugins`，引擎默认 no-op）。
- `pii` — 检测规则、映射 TTL、恢复缓冲上限。目标被标为 `external_targets` 但 PII 未启用
  时，请求 fail-closed（503）。
- `scheduler` — 虚拟节点、健康探测、指标抓取、**负载感知候选数 / 等待阈值**、
  `breaker_error_threshold` / `breaker_open_for`（实例级被动熔断）。
- `connector` — 出站 HTTP transport：per-host 空闲池、`MaxConnsPerHost` 反压、HTTP/2。
- `server` — `addr`、`read_timeout`、`write_timeout`、`read_header_timeout`（slowloris
  防护，流式 tail 豁免）、可选 `tls_crt` / `tls_key`。
- `eventbus` — worker 数 + 缓冲（默认 no-op sink）。


## 测试

```bash
make test        # 单元测试
make test-race   # 带 -race
make vet
```

## 扩展

- **Context 插件**：实现 `context.Plugin`（`Produces` / `Consumes` / `Execute`）；引擎根据
  声明推导 DAG 边。
- **服务发现**：实现 `scheduler.InstanceSource`（K8s / Consul / etcd / Nacos watch）。
- **共享状态**：`pii.MapStore` 与 `context.BreakerStore` 可接入 Redis 适配器以实现跨副本
  一致性。
- **事件 sink**：实现 `eventbus.Sink` 对接 Kafka → ClickHouse；默认 `NoOpSink` 丢弃事件。
- **命中反馈适配**：`cacheHitTap` 解析 OpenAI 标准 `usage.prompt_tokens_details.cached_tokens`；
  非标准后端可替换 `extractCacheHitFraction`。

## 非目标

Embedding、Graph 查询、Agent Runtime、Workflow、Memory 存储、知识图谱推理不由网关
提供——它们由插件或上层 Agent 层承担。
