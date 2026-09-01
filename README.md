**JiuwenSwarm extension to support multi-modal creation/design scenarios and designer-users.**

This README is intentionally broken to mark WIP state.

This README will be restored once this branch is ready as a PR.

This branch is always (re-)based on `img-vid-gen-inline` and/or other branches that supports image and video generation provider configuration.

大致架构实现方向：

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