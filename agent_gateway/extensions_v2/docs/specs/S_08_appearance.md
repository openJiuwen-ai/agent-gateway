# 受控外观规约

## 元信息

| 项 | 内容 |
| --- | --- |
| 类型 | spec |
| 编号 / slug | `S_08 / appearance` |
| 所属模块 | Gateway `webui/`（接口、校验、分层记录、合成与投递）；Web 前端 `features/webui/`（派生与应用、主页品牌显示） |
| 最近一次修订日期 | 2026-11-23 |
| 契约状态 | 配色派生（第 3.1、3.2 节）已实现。外观部件、分层叠加、快照帧与主页品牌显示（第 1～6 节，EXTV2-GW-16）：Gateway 侧与 jiuwenswarm 前端均已实现，尚未合入；新帧与旧前端不兼容，两侧须随同一次 agent-gateway 固定提交升级交付（[F_16](../features/F_16_layered-appearance-and-brand.md) 第 7 节） |
| 关联文档 | [F_11](../features/F_11_controlled-appearance.md)、[F_16](../features/F_16_layered-appearance-and-brand.md)、[S_06 浏览器桥接](S_06_local-browser-bridge.md)、[S_07 媒体容器](S_07_media-surface.md)、[S_09 启动门控](S_09_startup-splash.md) |

本文维护 `ops.appearance.apply` 的接口、外观部件、宿主派生与显示规则、下行帧、层级与生命周期及不变量。

## 1. 接口

```python
await ops.appearance.apply(appearance: Mapping | None) -> dict
```

- 调用方须是活动的应用级 UI 包（[S_06](S_06_local-browser-bridge.md)），调用须属于该包当前激活，
  且发生在带浏览器连接的事件中（如应用级槽位的 `ui.ready`、`ui.click`）。外观作用于该连接。
- 外观由部件组成（第 2 节），包只能设置、撤回自己的部件：

| `appearance` | 语义 |
| --- | --- |
| 以部件为键的对象 | 至少一个键；值为对象时设置本包的该部件，为 `null` 时撤回本包的该部件；未出现的部件保持本包原值 |
| 只含第 2.1 节配色字段的对象 | 等同于 `{"colors": <该对象>}` |
| `None` | 撤回本包的全部部件 |

- 经事件链执行：Ops 只触发 `ui.appearance`（输入携带调用包 `package_id` 供匹配与查看；调用方由宿主在事件之外传递，见 S_01 第 4 节），USER Handler 可以拒绝或接管（返回自己的 `HostActionOutput`，例如 `{"status": "denied", "code": …}`，Ops 原样返回），继续后由 CORE 默认动作核对激活与连接再执行；CORE 拒绝时 Ops 抛出 `ValueError`。
- 返回 `{"status": <状态>, "revision": <int>, "shown": [<部件>…]}`。`shown` 为调用后本包在该连接上生效的部件，
  按第 2 节的顺序排列；`revision` 为已下发或当前的快照版本：

| 状态 | 含义 |
| --- | --- |
| `applied` | 至少设置了一个部件；合成结果变化时已下发新快照，否则不下发 |
| `revoked` | 只撤回了本包原有的部件 |
| `unchanged` | 调用没有改变本包的记录（撤回本包没有的部件） |
| `delivery_failed` | 合成结果变化但连接已不可用；本包的记录保持调用前的状态 |

- 外观对象不合法、品牌图片不符合第 2.7 节、调用方不是活动的应用级 UI 包、调用属于旧激活或没有浏览器连接时
  抛出 `ValueError`，记录与快照都不变。

## 2. 外观部件

| 部件 | 内容 | 定义 |
| --- | --- | --- |
| `colors` | 强调色、表面模式与装饰变量 | 第 2.1 节 |
| `identity` | 品牌名、侧栏 Logo、浏览器标签图标 | 第 2.4 节 |
| `welcome` | 欢迎语的后半句 | 第 2.5 节 |
| `mascot` | 主页吉祥物 | 第 2.6 节 |

每个部件整体生效：部件内省略的字段取宿主默认，不从下层同部件补齐（第 5 节）。对象不接受表外的键，
部件键与配色字段不能出现在同一对象中。

### 2.1 配色 `colors`

| 字段 | 类型 | 必填 | 约束 |
| --- | --- | --- | --- |
| `accent` | string | 否 | `#RRGGBB` |
| `surface` | `opaque` / `translucent` | 否 | 省略按 `opaque` |
| `surfaceOpacity` | number | 否 | 0.3～1；只能与 `surface: "translucent"` 同时提供；省略按 0.86 |
| `tokens` | object | 否 | 至少一项；键取自第 2.2 节白名单，值为第 2.3 节的颜色 |

对象至少包含一个字段，不接受表外字段。

### 2.2 token 白名单

`--color-scrollbar-thumb`、`--color-sidebar-nav-active`、`--color-sidebar-nav-active-hover`、
`--color-conversation-active-surface`、`--color-chat-accent-subtle`、`--color-action-primary-subtle`、
`--color-action-primary-glow`、`--color-border-accent`。

白名单只含装饰性高亮变量；文本、表面与弹出层变量由宿主维护，插件不能直接修改。

### 2.3 颜色

`#rgb`、`#rrggbb`、`#rrggbbaa`（十六进制，大小写不限），或 `rgb(r, g, b)`、`rgba(r, g, b, a)`：
r、g、b 为 0～255 的整数，a 为 0～1 的数，逗号两侧可有空格。不接受其他写法。

### 2.4 标识 `identity`

至少一个字段。

| 字段 | 约束 | 用途 |
| --- | --- | --- |
| `name` | 文本，1～32 个字符 | 浏览器标签标题、欢迎语中高亮的品牌名、门控中性层的名称 |
| `logo` | 图片 | 侧栏 Logo、门控中性层的 Logo |
| `favicon` | 图片；省略时取 `logo` | 浏览器标签图标 |

### 2.5 欢迎语 `welcome`

至少一个字段。欢迎语为“品牌名 + 后半句”，本部件只提供后半句。

| 字段 | 约束 |
| --- | --- |
| `default` | 多语言文本，每种语言 1～64 个字符 |
| `modes` | 对象，键取 `work`、`code`、`design`（宿主的工作模式），值为多语言文本；至少一项 |

后半句的取值顺序：当前工作模式的 `modes` 项、`default`、宿主对该模式的默认文字。

### 2.6 吉祥物 `mascot`

| 字段 | 约束 |
| --- | --- |
| `src` | 必填，图片 |
| `hoverSrc` | 可选，图片；悬停时显示 |
| `hoverMs` | 与 `hoverSrc` 同时出现；500～10000 的整数，悬停图的显示时长（毫秒） |

### 2.7 文本与图片

- 多语言文本：`{"zh"?: string, "en"?: string}`，至少一项。界面语言以 `zh` 开头时取 `zh`，否则取 `en`，缺失时取另一项。
- 文本：去掉首尾空白后非空，不含控制字符与换行，长度按字符计；校验后的值是去掉空白后的文本。
- 图片：站内素材路径，通过与 `MediaSurface` 相同的路径校验（S_07 不变量 2），扩展名为 `.png`、`.webp`、`.svg` 或 `.gif`。
  Gateway 另外核对：路径属于调用包当前激活的包素材（`/extension-assets/<包>/<代次>/…`）或数据素材
  （`/extension-assets/<包>/<代次>.data/…`），即 `ops.ui.asset_url` 为它生成的地址；文件存在且不超过上限：
  `logo`、`favicon` 1 MiB，`src`、`hoverSrc` 各 4 MiB。大小只在提交时检查。
- 浏览器与 Gateway 对形态的校验一致（第 7 节的共享用例）；所有权与文件大小只在 Gateway 检查。

## 3. 宿主派生与显示

### 3.1 强调色

宿主由 `accent` 派生强色：将其逐步与黑色混合，直到与白色的对比度不低于 4.5:1；悬停色在强色基础上与白色混合，
保持与白色的对比度不低于 3:1。

| 变量 | 取值 |
| --- | --- |
| `--color-action-primary`、`--color-border-focus`、`--color-chat-accent`、`--color-chat-send-active`、`--color-switch-track-on`、`--color-settings-switch-checked`、`--color-skill-capsule-active`、`--color-conversation-unread`、`--color-settings-link`、`--color-cron-action-link`、`--color-text-link` | 强色 |
| `--color-action-primary-rgb` | 强色的 `r, g, b` |
| `--color-action-primary-hover` | 悬停色 |
| `--color-action-primary-subtle`、`--color-chat-accent-subtle`、`--color-action-primary-glow`、`--color-border-accent` | `accent` 不透明度 0.12、0.14、0.16、0.5 |
| `--color-border-accent-rgb` | `accent` 的 `r, g, b` |

`tokens` 在强调色派生之后应用，同名时以 `tokens` 为准。

### 3.2 半透明模式

只在 `surface` 为 `translucent` 且应用背景显示（S_07 的 `data-webui-background="active"`）时生效。
设不透明度为 o，基色取当前主题对应变量的颜色：

| 表面 | 不透明度 |
| --- | --- |
| 页面底色（`--color-surface-page`、`--color-surface-page-accent`、`--color-extension-section-surface`） | o × 0.42 |
| 侧栏与导航（`--color-sidebar-surface`、`--color-settings-nav-surface`、`--color-surface-chrome`、`--color-rsi-canvas-bg`） | o × 0.7 |
| 内容与面板（`--color-surface-content`、`--color-surface-panel`、`--color-rsi-page-bg`） | min(1, o + 0.02) |
| 卡片（`--color-surface-card`、`--color-surface-elevated`） | min(1, o + 0.08) |
| 外壳（`--color-surface-shell`） | 透明 |

主题按角色提供的表面变量同样只在根上覆盖：

| 角色变量 | 半透明时 |
| --- | --- |
| `--color-surface-workspace`（大面积工作区容器） | 卡片基色 × o × 0.7 |
| `--color-surface-workspace-layer`（叠在工作区上的内层） | 透明 |
| `--color-chat-composer-context-surface`（新建任务输入框的项目上下文外层） | 透明；不透明模式沿用次要操作区底色 |
| `--color-surface-floating`（浮起的侧栏与设置导航） | 侧栏基色 × 0.9 |
| `--effect-surface-floating-backdrop`（浮起侧栏的磨砂） | `blur(24px) saturate(1.3)`，不透明模式为 `none` |
| `--color-surface-popover`（菜单、对话框、抽屉与提示） | 卡片基色 × 0.97 |

宿主另使页面根与应用根透明。外观样式只写这些变量，不出现任何业务组件的类名；各组件消费变量（主题文件中定义默认值），
新增弹出层只需使用 `--color-surface-popover`。全部文本变量不变。

聊天输入框的 `--color-chat-composer-surface` 引用 `--color-surface-workspace`，与工作区使用相同的表面规则。
右侧工具面板的外层使用工作区表面，内层和分节标题使用 `--color-surface-workspace-layer`，避免多层半透明底色
叠加后接近纯白；浮起的工具面板使用 `--effect-surface-floating-backdrop` 保持背景动画上的文字可读。

派生只读取配色部件；`colors` 缺省时使用宿主主题。

### 3.3 主页品牌显示

| 位置 | 数据 | 规则 |
| --- | --- | --- |
| 侧栏 Logo | `identity.logo` | 宿主固定尺寸，按比例完整显示；加载失败时显示宿主 Logo |
| 浏览器标签 | `identity.name`、`identity.favicon` | 标题为品牌名，缺省为 “WorkSwarm”；图标缺省为宿主图标 |
| 欢迎语 | `identity.name`、`welcome` | 高亮词为品牌名，缺省为 “WorkSwarm”；后半句按第 2.5 节取值 |
| 吉祥物 | `mascot` | 显示区域沿用宿主尺寸，按比例完整显示；悬停交互与宿主默认吉祥物相同，有 `hoverSrc` 时在 `hoverMs` 内显示悬停图；加载失败时显示宿主默认吉祥物 |

- 标识在门控的外观准备结束前取上次的首帧记录，之后取当前快照（[S_09 第 4、5 节](S_09_startup-splash.md)）；欢迎语与吉祥物只取当前快照。
- 吉祥物悬停时宿主照常显示原有的悬停提示；没有 `hoverSrc` 时悬停不换图。部件更换后吉祥物回到静止图。
- 图片加载失败只对该地址回退宿主图片；部件换成新地址后重新尝试。
- 前端没有 Gateway 连接（HTTP 传输）时没有门控与外观，标签恢复宿主标题与图标。

文字只按纯文本显示。门控期间的中性层与标签见 [S_09 第 5 节](S_09_startup-splash.md)。群聊创建页的欢迎区不受外观影响。

## 4. 下行帧

事件 `webui.appearance`，载荷是该连接的完整快照：

```json
{
  "revision": 7,
  "parts": {
    "colors":   { "package_id": "pkg.theme", "value": { "accent": "#b45309" } },
    "identity": { "package_id": "pkg.logo",  "value": { "name": "Acme", "logo": "/extension-assets/pkg.logo/<代次>/logo.svg", "favicon": "/extension-assets/pkg.logo/<代次>/logo.svg" } }
  }
}
```

- `parts` 只含有提供者的部件，未出现的部件用宿主默认；空对象表示全部为宿主默认。
- `value` 为校验后的规范形式：只含配色字段的提交已转为 `colors`，`favicon` 缺省时已补为 `logo`，文本已去掉首尾空白。
- `revision` 在同一连接内递增；浏览器在同一连接内忽略不大于已接受值的帧。载荷不合法时浏览器忽略该帧。

## 5. 层级与生命周期

层级取自外观层级记录：应用级 UI 包首次发布时追加到记录末尾，记录中越靠后层级越高；不在记录中的包位于全部已记录包之下。
每个连接上，每个部件显示提交了该部件、层级最高的活动包的值。

| 情况 | 行为 |
| --- | --- |
| 包设置部件 | 记住本包在该连接上的值；合成结果变化时下发快照 |
| 包撤回部件或提交 `None` | 删除本包的值；若它正在显示，下一层的值或宿主默认随之显示 |
| 下层包提交 | 记住其值；上层仍提供该部件时快照不变、不下发 |
| 包释放（停用、卸载、重载） | 删除该包在全部连接上的值；向它在其上有值或外观帧仍在发送中的连接预留新的 `revision`，在连接锁内下发重新合成的快照，排在在途帧之后；向浏览器清除该包的应用级实例 |
| 重载成功 | 广播 `extensions.v2.installed`，已挂载的槽位重新发送 `ui.ready`；层级不变 |
| 连接关闭 | Gateway 丢弃该连接的全部值与版本 |
| 浏览器断线 | 保留当前外观；重连后应用级槽位重新发送 `ui.ready`，全部返回后仍未收到快照时浏览器恢复宿主默认 |

| 层级记录 | 规则 |
| --- | --- |
| 保存位置 | `<data_dir>/host/appearance.json`：`{"version": 1, "installed": [...]}`，原子替换写入 |
| 记录建立前已安装的包 | 按首次启动时的发布顺序 |
| 生命周期 | 重载、升级与重启保留；卸载时删除；文件无法读取时按空记录处理并记录警告；写入失败只记录警告，下次变化时重写 |

恢复宿主默认只移除外观贡献层，不改变 `data-theme` 与 `data-color-mode`。

## 6. 不变量

1. 外观只能改变第 3 节列出的变量、页面根与应用根的背景，以及第 3.3 节的品牌位置；不接受任意 CSS、选择器、`url()` 或 `var()`。
2. 文本颜色不受外观影响，弹出层在半透明模式下保持 0.97 的近不透明；派生的按钮主色与白色文字的对比度不低于 4.5:1。
3. 半透明只在应用背景显示时生效，背景移除即恢复不透明表面。
4. 包身份与连接来自调用上下文；包不能为其他包或其他连接设置或撤回部件。
5. 每个连接上，每个部件至多一个生效提供者，即层级最高且提交了该部件的活动包；生效结果只取决于各包当前的值与层级记录，
   与提交、应答的先后无关。
6. 撤回与提供者切换不改变用户的明暗主题选择。
7. 包释放后，浏览器最终显示的快照不含该包的任何部件：释放时仍在发送的外观帧同样会被一个更大 revision 的快照覆盖；
   释放前已通过激活核对、仍在等待连接锁的调用，取得锁后再次核对激活，失败则不发送任何帧（抛出 `ValueError`），
   因此不会出现比该快照 revision 更大、仍含该包部件的帧。
8. 品牌图片只来自其提供者自己当前激活的素材；文字只按纯文本显示。

## 7. 验证入口

| 契约 | 用例 |
| --- | --- |
| 部件校验（两端一致） | `gatewayAppearanceCases.json`（前端 `tests/fixtures` 中为同一份副本）；`test_gateway_appearance.py`；前端 `test:webui-components`（`Gateway appearance contract` 用例） |
| 接口、层级、叠加与释放 | `test_gateway_appearance.py` |
| 层级记录 | `test_gateway_appearance.py`（`test_the_layer_order_is_kept_and_forgotten_on_disk`） |
| 品牌图片所有权与大小 | `test_gateway_appearance.py` |
| 帧处理、断线与派生 | 前端 `test:webui-runtime`（快照校验、版本顺序、暂存与重连）、`test:webui-components`（派生与外观宿主） |
| 主页品牌显示 | 前端 `test:webui-components`（主页品牌位置、欢迎语取值、图片回退用例） |
