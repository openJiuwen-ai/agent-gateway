**JiuwenSwarm extension to support multi-modal creation/design scenarios and designer-users.**

This README is intentionally broken to mark WIP state.

This README will be restored once this branch is ready as a PR.

This branch is always (re-)based on `img-vid-gen-inline` and/or other branches that supports image and video generation provider configuration.

最终架构实现方向（**不一定现在就采用。做plugin抽离又是额外的架构考量。目前快速prototype直接写进主代码仓好了**）：

- 目前主仓并没有提供含UI能力的拓展机制。只有：
  - Plugin Package（plugin_packages，可热装）：Skills + Tools，对话里按包装配
  - Harness Package：Tools / Skills / Rails，热激活
  - Agent Template / Group：人设、角色组
  - Skill / Swarm Skill：单 Agent 或团队协作流程
  - `jiuwenswarm/extensions/`（`extension.yaml`）：后端钩子（如 AgentServerClient、ThirdAgent）
  - A2UI：聊天里的生成式表单/卡片
  - Settings overlay：设置页模块组合（偏内建扩展）
- 在现有现状下，我们的实现全量合入主仓，默认随发行可用。
  - 前端：扩写现有 channels/web/frontend（导航、Designer 空间、画布、prompt→project 跳转）。
  - Agent 侧能力：放在 `jiuwenswarm/resources/agent/workspace/plugins/plugin_packages/<你的模块>/`（skills / tools，可参考 content-creation），作为内置 package，不是用户另装。
  - 还需要进主仓、但通常不塞进 plugin_packages 的，是壳与编排：例如 `work_mode`/路由、project API、gateway handler、画布图执行/状态——这些跟现有 server/web 代码长在一起。

----------

----------

----------

**目前可快速上手的大致架构和合作建议**

把「执行图」拆成三层，Python 侧放在 `jiuwenswarm/common/schema/`，前后端通过 WS RPC + 契约测试对齐；UI 和 executor 只依赖同一套 domain model，不互相 import。

这和 README 里「壳与编排跟 server/web 长在一起」、以及仓库现有惯例（`common/schema/` 为 canonical、前端 hand-mirror TS、pytest 做跨层比对）是一致的。

---

## 1. 放哪里：三层，不要混成一份

你们实际需要 **三种数据**，职责不同：

| 层 | 是什么 | 放哪里 | 谁消费 |
|---|---|---|---|
| **A. Domain Graph（真相源）** | 节点/边/依赖/执行语义 | `jiuwenswarm/common/schema/designer_graph.py` | 后端 executor + 前端类型镜像 |
| **B. Run State（运行时状态）** | 每个 node 的 running/done/failed、输出引用 | `jiuwenswarm/server/runtime/designer/`（或 `session/`） | executor 写，前端订阅 |
| **C. View State（展示层）** | React Flow 的 `position`、`selected`、viewport | 前端 `features/designer/` | 仅 UI；可选持久化到 graph metadata |

**关键原则：React Flow 的 `nodes/edges` 不是真相源，只是 View 投影。**

```
                    ┌─────────────────────┐
  prompt/bootstrap  │  DesignerExecutionGraph │  ← schema_version, nodes[], edges[]
                    └──────────┬──────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
     toReactFlowView()   GraphExecutor    SubagentRail / tools
     (UI 投影)           (调度/对齐/合并)   (真正跑 agent)
              │                │
              ▼                ▼
     ReactFlow nodes/edges   ExecutionRun + node_states
                             (WS events 推给前端)
```

Symphony 里已有 `execution_graph { nodes, edges }` 和 `workflow_state.py` 的运行态模式，可以 **借鉴 shape**，但不要直接复用 schema——Symphony 是 skill 拓扑，你们是 **Designer 多媒体创作 DAG**。

---

## 2. Domain Graph 建议长什么样

第一版地基尽量小，但字段要够你们后面扩展：

```python
# jiuwenswarm/common/schema/designer_graph.py

SCHEMA_VERSION = "designer-execution-graph.v1"

# 节点类型常量（契约测试会 pin 这些字面量）
NODE_TYPE_OVERVIEW = "overview"
NODE_TYPE_CHARACTER_DESIGN = "character_design"
NODE_TYPE_STORYBOARD = "storyboard"
NODE_TYPE_FRAME_GEN = "frame_gen"
NODE_TYPE_CLIP_GEN = "clip_gen"
# ...

EDGE_KIND_DATA = "data"        # 产出物传递
EDGE_KIND_SYNC = "sync"        # 对齐/barrier（两 subagent 互相对齐）
EDGE_KIND_CONTROL = "control"  # 纯依赖，无数据
```

```json
{
  "schema_version": "designer-execution-graph.v1",
  "graph_id": "graph_abc123",
  "project_id": "proj_xxxx",
  "title": "短视频：赛博朋克街景",
  "source": "prompt",
  "nodes": [
    {
      "id": "n_overview",
      "type": "overview",
      "label": "项目概览",
      "config": { "prompt": "..." },
      "layout": { "x": 0, "y": 0, "width": 320, "height": 180 }
    },
    {
      "id": "n_char",
      "type": "character_design",
      "label": "角色设计",
      "config": { "agent_template": "character-designer", "inputs": ["n_overview"] }
    }
  ],
  "edges": [
    { "id": "e1", "source": "n_overview", "target": "n_char", "kind": "data" },
    { "id": "e_sync", "source": "n_char", "target": "n_story", "kind": "sync" }
  ]
}
```

**和 React Flow 的分工：**

- Domain：`id`, `type`, `config`, `layout`（可选持久化位置）
- React Flow：从 domain 映射出 `position`/`type`/`data`；选中态、拖拽中的临时坐标可以不立刻写回
- **素材本体不进 node 大 payload**：node 只存 `asset_ref` / `output_ref`，大文件走 project 存储或 artifact API

Run State 单独一份（类似 `workflow_runs`）：

```json
{
  "run_id": "run_xxx",
  "graph_id": "graph_abc123",
  "status": "running",
  "node_states": {
    "n_char": { "status": "running", "started_at": 123, "output_ref": null },
    "n_story": { "status": "waiting_sync", "blocked_by": ["n_char"] }
  }
}
```

---

## 3. 前后端怎么对接（沿用现有惯例）

仓库没有 protobuf/OpenAPI codegen，做法是：

| 项 | 位置 |
|---|---|
| Python canonical | `jiuwenswarm/common/schema/designer_graph.py` |
| RPC 方法名 | `jiuwenswarm/common/schema/message.py` → `ReqMethod` |
| Gateway handlers | `jiuwenswarm/gateway/.../app_web_handlers.py`（参考 `project.*`） |
| 持久化 | `server/runtime/designer/graph_store.py`（参考 `project_store.py`） |
| 前端类型 | `channels/web/frontend/src/features/designer/executionGraphTypes.ts` |
| 前端 client | `designerGraphClient.ts`（参考 `projectRegistryClient.ts`） |
| 契约测试 | `tests/unit_tests/test_designer_execution_graph_contract.py` |

建议第一版 RPC（够两人并行）：

```
designer.graph.get          # 读 domain graph
designer.graph.save         # 写 domain graph（含 layout）
designer.graph.bootstrap    # prompt → project + 初始 graph
designer.run.start          # 从某 node 或整图开跑
designer.run.get            # 读 run state
designer.run.pause / cancel
```

事件（WS push，参考 `chat.subtask_update`）：

```
designer.node.updated       # 单 node 状态变化
designer.run.updated        # run 级状态
designer.graph.updated      # 图结构被 agent 改写（可选）
```

---

## 4. 地基 PR 做到什么程度，够 UI / Agent 分头干

目标：**两人只依赖 schema + stub API + fixture，不互相 block。**

### 必须做（第一地基 PR）

1. **Schema + 常量 + 示例 fixture**
   - `designer_graph.py` + `executionGraphTypes.ts`
   - `tests/fixtures/designer-execution-graph.v1.json`（含：概览 → 双 subagent 对齐 → 合并 → 分头生成）
   - 契约测试：node types、edge kinds、`schema_version`、必填字段

2. **Graph ↔ React Flow 纯函数 adapter（前端）**
   - `toReactFlowGraph(domainGraph) → { nodes, edges }`
   - `fromReactFlowGraph(rf, domainGraph) → domainGraph`（只回写 layout + 结构变更）
   - 用 fixture 单测，**不依赖后端**

3. **Graph store + RPC stub（后端）**
   - `get/save/bootstrap` 能读写 JSON
   - `run.start/get` 先返回 mock 状态机（例如 2s 后把 node 标成 done）
   - executor 接口先定：`GraphExecutor.run(graph, run_id) -> AsyncIterator[NodeEvent]`

4. **Executor 接口，不实现具体 subagent**
   ```python
   class NodeHandler(Protocol):
       async def execute(self, node, ctx) -> NodeResult: ...

   NODE_HANDLERS: dict[str, NodeHandler]  # type -> handler
   ```
   Agent 同学只需填 `character_design` / `storyboard` 等 handler；UI 同学不用碰。

5. **Project 绑定**
   - 每个 graph 挂 `project_id`（复用 `project_store`）
   - `designer.graph.bootstrap`：`prompt → create project → 生成初始 graph`

### 可以第二轮再做

- 真实 subagent 调度（SubagentRail 集成）
- `sync` 边的 barrier 逻辑
- 画布多媒体 preview / NodeToolbar
- Agent 自动改图（graph patch / merge）

### 建议两人分工切线

| 你（UI / React Flow） | 合作者（Agent / 执行流） |
|---|---|
| `features/designer/` 页面、React Flow 画布 | `server/runtime/designer/executor.py` |
| `toReactFlowGraph` / 节点组件 stub | `NODE_HANDLERS` 各 type 实现 |
| 订阅 `designer.node.updated` 染状态 | `sync` / `merge` 调度策略 |
| layout 编辑 → `designer.graph.save` | prompt → 初始 graph 的生成逻辑 |

**唯一共享面：fixture JSON + TypeScript types + Python schema + RPC 名。**

---

## 5. 几个容易踩坑、现在就该定下来的点

1. **Graph 定义 vs Run 状态必须分开**  
   否则 UI 拖拽保存会把 `running` 状态写乱，或 rerun 时清不干净。

2. **Node `config` 要 typed，别全是 `Record<string, unknown>`**  
   按 `type`  discriminated union；前后端各一份，契约测试 pin 合法 type 列表。

3. **对齐边（sync/barrier）是一等公民**  
   角色设计 + 分镜设计「互相对齐」不要硬编码在 executor if-else，用 `edge.kind = "sync"` + scheduler 读入度/屏障。

4. **素材引用，不是素材本体**  
   node 里放 `output_ref: { kind: "image", uri: "..." }`，播放/预览 UI 再 resolve。

5. **不要放进 plugin_packages**  
   按 README，壳与编排在 `server/web`；plugin_packages 只放 skills/tools（如「重新生成这张图」的具体 tool）。

6. **别和 Symphony skill graph 混存储**  
   可以抄 `SkillGraphPanel` 的布局思路，但 Designer graph 独立 `schema_version` 和存储路径。

---

## 6. 建议的第一批文件清单

```
jiuwenswarm/common/schema/designer_graph.py
jiuwenswarm/common/schema/message.py                    # + ReqMethod
jiuwenswarm/server/runtime/designer/graph_store.py
jiuwenswarm/server/runtime/designer/executor.py         # 接口 + mock
jiuwenswarm/server/runtime/designer/handlers/__init__.py
jiuwenswarm/gateway/.../app_web_handlers.py             # designer.graph.* / designer.run.*

jiuwenswarm/channels/web/frontend/src/features/designer/
  executionGraphTypes.ts
  designerGraphAdapter.ts      # domain <-> React Flow
  designerGraphClient.ts
  fixtures/designer-execution-graph.v1.json

tests/unit_tests/test_designer_execution_graph_contract.py
```

---

**一句话：地基 =「版本化 domain schema + graph/run 分离 + RPC 壳 + React Flow adapter + 一份真实 fixture + mock executor」。**  
有了这个，你可以先做画布和状态染色，合作者可以并行做 node handler 和调度，只在 fixture 和 RPC 上对齐。

如果你愿意，下一步我可以直接按这个清单帮你起草 `designer_graph.py` 和 fixture JSON 的 v1 字段定义。

