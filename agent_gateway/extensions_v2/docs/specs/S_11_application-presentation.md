# 应用形态

## 槽位与身份

AP-01：`application.presentation` 是 Gateway 所有、按可信 Web 连接隔离的应用级列表槽位。
只接受 S_17（jiuwenswarm 仓 `jiuwenswarm/extensions_v2/docs/specs/S_17_webui-component-props.md`） 定义的 `ApplicationSurface` 根。
插件须声明 `package.domain: gateway` 及该槽位。Server 不接受该槽位。
插件的静态页面必须使用同包、当前激活代次的 `/extension-assets/` URL。

AP-02：每个已发布的 `ApplicationSurface` 实例登记一个形态候选，身份由包与 instance_key 决定。
桌面宿主保留系统原生窗口标题栏，在工作台页面右下角提供“切换应用形态”悬浮按钮，菜单列出需手动启动的候选；选择后只打开该候选的小窗。
默认情况下安装、资源导入及设置保存只登记或更新候选，不自动进入；资源包格式、导入和配置由插件定义。
autoStart 缺省 false；两种 mode 设为 true 时随挂载直接显示且不进入切换菜单，停用或卸载时关闭，应用重载后重新显示。
用户或智能体可以在安装成功后另行调用下述本机启动入口，复用菜单选择的同一窗口流程。
形态根的 mode 缺省为 desktop，仅此模式需要桌面客户端。overlay 在浏览器和桌面客户端内运行，工作台始终可见。
主窗口继续拥有连接和聊天状态，隐藏期间保持后台调度；小窗不创建聊天连接。收到沙箱 `ready` 后才隐藏主窗口。
工作台恢复、窗口关闭、插件停用或卸载、主页面重载与窗口加载失败均结束当前窗口代次；过期消息不执行。
断线时停止接受写入并恢复工作台。错误须可见，不能把未知发送结果当作成功后自动重发。

## 应用内悬浮承载

AP-09：overlay 覆盖应用视口，切换页面和会话保留同一 iframe，最小化时随所属窗口隐藏；复用原候选、会话快照、沙箱和请求协议。
一个连接只激活一个形态，切换先结束旧形态。mode 或 src 更新结束当前容器，autoStart=true 时按新声明显示；config 更新保留页面。
关闭、清除、停用、卸载、断线或加载失败释放容器及资源。手动关闭后普通更新不重新显示，可显式启动、重新启用或重载应用；自动启动不替换已有活动形态，容量占用时提示关闭当前形态后重新启用。
容器矩形内由插件接收输入，外部由应用接收；透明像素不提供逐像素点击穿透。拖动接入示例见 Swarm 的 `docs/zh/application-presentation.md`。

## 插件设置与关闭通知

插件通过 `ui.change` 处理设置，用 `ops.ui.render` 发布 autoStart 声明或 CLEAR，并维护显示状态；开启时保留当前页面。`DELIVERED` 仅表示帧已下发，`visible_when` 仅控制挂载条件。
容器关闭后通过 `webui.dispatch` 发送 `ui.closed`，target 指定原包、实例和 application 作用域，payload 严格包含：

| 字段 | 含义 |
| --- | --- |
| `reason` | `closed`（含返回主工作台时关闭桌面形态）、`ready_timeout`、`render_failed`、`load_failed`、`renderer_gone`、`transition_failed` 或 `open_failed` |
| `revision` | 容器关闭时对应的渲染版本，正安全整数 |
| `presentation_id` | 连接内递增的正安全整数；每次打开分配新值 |

ready 前的打开失败报告 `open_failed`；替换、清除及宿主释放不重复通知。Gateway 核对活动连接、包、实例和版本，拒绝重复或过期关闭，结束匹配的启动等待；候选由插件决定是否 CLEAR。插件核对当前版本后更新显示状态和设置页。
关闭通知绕过 USER 请求策略，处理预算 5 秒，容器关闭不依赖处理结果。断线通知 `gateway.disconnect` 后清理连接及关闭记录。

## 本机启动入口

`jiuwenswarm presentation query [package_id] --json` 查询当前实例 Gateway 已发布的形态候选。
每项包含 `connection_id`、`package_id`、`instance_key`、`revision` 和双语 `title`，不包含会话正文或凭据。
desktop 候选启动到浏览器时返回 `DESKTOP_REQUIRED`。

`jiuwenswarm presentation start <package_id> <instance_key> --connection-id <id> --revision <revision> --json`
只启动查询中明确指定的连接及版本，不猜测首个连接或最新资源。DMG/EXE 使用
`JIUWENSWARM_EXECUTABLE` 对应后端程序，参数不变；本机实例发现与授权复用 extension CLI。

Gateway 重新校验连接、活动包和当前候选，将一次性请求发送给该连接。
Web Client 再校验当前渲染版本，复用菜单的 activate；对应承载容器 ready、会话选择成功后才回报 `started`。
同一连接同时只允许一次启动；启动中返回 `BUSY`。目标已更新、释放或连接结束时拒绝旧请求。
已启动的同一候选再次显式启动不重建窗口。安装、导入及保存不会自行调用此入口。

下行事件为 `webui.presentation.start`，字段为 `request_id`、`target`、`revision`、`deadline`（UTC Unix 毫秒）；过期通知不启动窗口。target 沿用应用级渲染目标，
不携带连接 ID、脚本、页面地址或原生窗口 ID。上行方法为 `webui.presentation.result`，字段为
`request_id`、`code`；code 为 `STARTED`、`DESKTOP_REQUIRED`、`TARGET_INACTIVE`、`BUSY`、`OPEN_FAILED`。
回执只接受原请求连接，且必须对应仍有效的活动包及渲染版本；USER 请求策略仍可以拒绝上行方法。
请求、Future 与连接锁由 Gateway 持有，只在当前实例内存在；断线、包释放、停机及等待结束时清理。

退出码 0 表示确认启动成功，1 表示明确拒绝，2 表示发出前输入或环境错误，3 表示结果未知。
30 秒内没有有效完成回执，或发出请求后连接/版本变化，返回 `UNKNOWN`，不自动重发。
未知结果须查看当前桌面；不能以通知已送达或 CLI 退出超时推断窗口是否已打开。

## 沙箱协议

AP-03：沙箱允许脚本和表单，不允许同源身份、顶层导航、Node 或预加载 API。只接受其 `contentWindow` 的消息。
消息形状为 `{protocol: "workswarm.presentation", version: 1, type, ...}`。不能通过插件提供的字符串执行代码。

| 方向 / type | 字段与语义 |
| --- | --- |
| 插件 → 宿主 / `ready` | 确认消息监听器已注册；可选 `transition: {durationMs}`，整数 200..2000，仅 desktop 可申请窗口过渡 |
| 宿主 → 插件 / `state` | `state`：语言、主题语义 token、连接状态、选中视图键、可见会话列表；`config`：插件声明的配置；`initialWorkArea`：首次创建时的工作区，desktop 为显示器正常桌面，overlay 为应用视口；overlay 另附 `host.bounds` 与 `host.workArea`，随容器几何变化更新 |
| 宿主 → 插件 / `transition` | `transition`：本代次唯一 id、direction（compact / workspace）、工作台 PNG data URL、workspace / surface 局部矩形、viewport 尺寸、workArea 正常桌面工作区；插件预加载并绘制首帧 |
| 插件 → 宿主 / `transition-ready` | `id`：确认该过渡首帧已就绪；宿主显示透明过渡区域并发出 `transition-start` |
| 宿主 → 插件 / `transition-start` | `id`：原生区域已经交接，插件开始计时绘制 |
| 插件 → 宿主 / `transition-finished` | `id`：末帧已绘制；宿主收成正常形态窗口或恢复工作台，随后发出 `transition-ended` |
| 插件 → 宿主 / `transition-failed` | `id`：绘制失败，结束该过渡并恢复工作台 |
| 宿主 → 插件 / `transition-ended` | `id`：窗口交接完成，插件释放截图和临时画布 |
| 插件 → 宿主 / `request` | `id`：本窗口代次内单调递增的正安全整数；`action`：下表枚举；`params`：严格匹配动作的对象 |
| 宿主 → 插件 / `result` | 原 `id`；`ok`；成功的 `value` 或失败的 `error`（稳定 code 与当前语言 message） |

overlay 的 `state` 消息附带 `host: {mode: "overlay", bounds: {x,y,width,height}, workArea: {x:0,y:0,width,height}}`，坐标为应用视口的 CSS px。`host` 随容器与视口变化更新，`initialWorkArea` 保留首次创建时的工作区。

| action | params | 行为 |
| --- | --- | --- |
| `select` | `conversationKey` | 只选择仍可见的完整会话目标；不恢复工作台 |
| `draft` | `conversationKey`, `text` | 当前目标的文本草稿，最多 32 KiB；不发网络请求 |
| `send` | `conversationKey`, `text` | 校验目标和发送状态后调用原有聊天入口；明确成功才清理该草稿 |
| `stop` | `conversationKey` | 使用已有停止入口 |
| `reply` | `conversationKey`, `replyId`, `offset`，可选 `role`（user / assistant，默认 assistant） | 读取选中会话最近一条用户消息或最后一条完整公开回复的一段；仅最近一条用户消息和最后回复，不是历史分页。offset 为 UTF-16 偏移，返回 `content` 和下一个偏移 `nextOffset`（结束时为 null）。 |
| `model` | `conversationKey`, `modelId` | 从宿主公开的可用单模型目录选择；运行中禁止切换，与工作台共用模型选择与 `session.selection.set` 保存入口；错误明确显示。 |
| `workspace` | 可选 `conversationKey`、`transition: {durationMs}` | desktop 选择目标后恢复完整工作台；overlay 定位会话并保留悬浮层；transition 仅 desktop 支持，时长限制同 ready，overlay 返回 UNSUPPORTED_ACTION |
| `resize` | `width`, `height` | 整数，宽 160..960、高 120..960；desktop 按显示器工作区约束；overlay 保留右下锚点并约束到应用视口，实际尺寸可小于请求值 |
| `move` | `deltaX`, `deltaY` | 有限整数、各 -4096..4096；移动当前承载容器；desktop 约束在显示器工作区，overlay 约束在应用视口 |
| `hitTest` | `interactive` | desktop：布尔值，交互区域接收点击，透明空白区域将点击传给桌面；宿主继续转发移动观察。overlay 返回 UNSUPPORTED_ACTION |

AP-08：窗口过渡仅在本地 Electron 与当前沙箱之间传递，提供给插件的截图仅包含本应用工作台；
宿主内部交接面可保持当前工作台或当前形态窗口的画面。
宿主根据两个原生窗口的实际矩形计算覆盖区域，矩形以逻辑像素表达，支持负屏幕坐标；
`initialWorkArea` 与 `workArea` 是宿主当前显示器的全局逻辑坐标，只读且不持久化；不携带插件的角色位置、布局或业务含义。
插件在过渡期间保持 `workArea`，避免系统全屏空间的浏览器可用高度变化导致角色在交接时再次限位；
无动画资源同样可使用 `initialWorkArea` 完成首次摆放，后续拖动仍按实际显示器处理。
系统全屏入口先保持来源画面，退出系统全屏并确认绘制后再完成 bootstrap；工作区取此时的正常桌面，
不使用全屏空间临时隐藏 Dock 后的可用区。该准备对无动画资源同样生效，不要求资源实现动画消息。
插件不提供截图来源、原生窗口 ID、屏幕区域或执行代码。截图不受普通会话帧 256 KiB 预算限制，
通过专用本地桥接传递，不进入会话请求结果或 Gateway。进入过渡前仍显示原工作台；
准备阶段由独立交接面保持当前来源窗口的像素画面；反向展开时，交接面实际显示后才隐藏原形态窗口，插件随后准备首帧。
插件首帧就绪后，宿主在交接面下调整动画窗口，分别等待透明准备帧和恢复可见状态后的绘制回执，
再隐藏来源工作台并移除来源交接面、通知插件播放。可见窗口不能在未覆盖时先放大，透明状态下的回执不能当作已显示。
不能用固定延时替代绘制回执。
收纳结束时，由独立的原生交接面保持插件提交的完整末帧；最终形态窗口调整尺寸并提交实际画面后再移除交接面。
过渡期间排队的移动、尺寸与命中更新在交接面保留期间执行并确认绘制，不能在撤去交接面后再次改变窗口。
交接面只呈现本地原生窗口截图，不重新绘制角色或动效，也不向插件增加艺术参数。
原工作台处于系统全屏时，覆盖窗口跨越该全屏空间和普通桌面；收到退出全屏事件后才隐藏原窗口，避免遗留空白空间。
反向展开保持覆盖，直到原工作台恢复原系统全屏状态且实际画面已提交后才释放。减少动态时同样保留全屏状态的往返交接。
结束信号必须匹配当前代次及活动过渡 id。准备或播放超过 15 秒、渲染失败、关闭、停用、卸载或断线时，
释放截图、计时器和过渡资源并恢复工作台，过期异步结果不操作新窗口。活动过渡期间窗口布局与命中更新等待交接完成。
宿主报告 reducedMotion 时不向插件提供动画截图或启动动画，直接完成窗口交接；全屏入口仍可由宿主保持来源画面准备正常桌面。插件同时停止非必要动画。
宿主不解释形态、角色、颜色、曲线或粒子参数。

AP-04：会话列表来自当前用户已经可见的会话；每项携带完整 `conversationKey`、会话标题、公开回复预览、`replyId`、`userMessageId`、最后一条用户消息摘要（最多 200 个 Unicode 码点）、草稿、运行状态和工具名。
`lastUserMessageAt` 是现有会话元数据 `last_user_message_at` 对应的 UTC Unix 毫秒时间，只读；新会话或来源缺失时为 null。宿主只归一化并透传该数据，不决定插件中的排序、数量或展示布局。
`inputMessageId` 是最近用户消息的稳定消息身份，`replyToMessageId` 是所提供回复对应的用户消息身份；两者可能不同，无法确认归属时为 null。`userMessageId` 与 `replyId` 是正文分段读取的版本标识，与稳定消息身份分开。`inputOutcome` 直接提供该输入已确认的 failed / stopped 结果，未有结果时为 null，不依赖短时提示计时器。
当前只列出单 Agent 会话，不登记 Team 子视图。状态优先级为需要用户输入、运行错误、仍有工具执行、正在思考、正在回复、暂停、完成、待命。宿主提供会话数据和请求结果，不将这些结果转换成某个插件的展示策略。
工具状态以未终结的工具执行集合为准，不以最后一条工具事件猜测。隐藏推理正文和凭据不进入插件状态。
草稿由主窗口持有并随目标隔离；收到新提醒不自动切换输入目标。目标删除或失去可见性后拒绝动作并清除其形态草稿。

AP-05：一个窗口代次内保留最近 512 个请求结果和已接受的最大序号。重复请求返回同一结果；同序号不同内容拒绝。
旧结果退出缓存后，该序号仍因小于最大序号而被拒绝，不能重新执行写入。最多同时接受 32 个未完成请求。
同一目标的并发发送被拒绝。窗口重建不重放旧请求。结果丢失时插件提示未知并由用户检查对话，不自动补发。

AP-06：完整最后回复与会话目录分开读取，不能以目录中的截断预览代替正文。
主窗口直接读取既有 Chat Store。`chat.final` 在消息分段去重前保留完整公开正文，并按请求标识记录对应的用户消息；较早输入的迟到结果不替换较新输入已经完成的结果。历史已恢复时根据原消息顺序建立回复与输入的关系。宿主不因运行、暂停、失败或停止隐藏已有公开回复；插件自行决定是否展示。
`replyId` / `userMessageId` 分别绑定该不可变公开正文对象；回复替换、目标变化或窗口代次变化后拒绝旧读取。每次 `reply` 最多返回 16 Ki UTF-16 代码单元，最坏 JSON 转义仍低于原生 256 KiB 单次传输上限。
插件顺序读取至 `nextOffset: null`，拼接后完整显示；不创建历史游标、消息列表、epoch 或分片缓存。长回复仅在自身区域滚动，输入、状态和模型选择保持可见。
示例插件只展示当前一轮：比较 `inputMessageId` 与 `replyToMessageId` 后选择回复，运行时显示状态，failed / stopped 结果显示明确提示；不重新使用上轮答案。回复标签、头像、布局与显隐均由插件决定。
形态未激活时不订阅会话 Store，也不投影会话消息。公开工具名、状态可以显示，隐藏推理正文不公开。

AP-07：示例插件的蜜蜂与胶囊提供运行状态、最后完整回复、输入、发送、停止和模型选择。
蜜蜂完全收起时只显示角色及操作按钮：有运行会话时显示数量和新建，没有运行会话时只显示新建。点击蜜蜂或数量先显示会话列表，运行中每页三条；无运行时由插件按 `lastUserMessageAt` 显示最近三条及查看全部入口，未知时间不参与最近排序。点击行在桌宠内展开对应会话，标题选择器选中该目标；只有显式工作台入口恢复应用。新建先选择现有的 `new` 目标，首次发送复用原有会话创建流程。按钮排布、计数、分页、收起及最近列表均属于插件。
需要回答问题或工具授权时打开原应用处理。全部历史、用户/助手消息列表与面板内回答/授权不属于当前版本。
当前仅提供单 Agent 会话，不提供模式切换；新会话固定为单 Agent。模型目录只公开稳定 ID 与展示名，不公开 API 地址和凭据。

## 不变量

- AP-I-01：插件样式、素材、轮播与布局不进入 Electron、Gateway 或聊天运行态。
- AP-I-02：只有主窗口能发布快照；只有当前形态代次能请求其有限承载能力；子 frame 不能调用原生 IPC。
- AP-I-03：插件卸载或小窗失败后，完整工作台仍可访问。
- AP-I-04：发送目标始终是请求中已校验的完整会话视图，不受随后提醒或工作台选择变化影响。

展开面板在同一滚动区域显示最近用户消息与最后回复全文。输入框右侧按钮在空闲时发送，运行或等待确认时显示方形停止图标；Enter 仅发送，不触发停止。状态行不放停止操作。

展开面板标题及会话选择菜单只切换桌面会话，不隐式打开主应用；独立“打开工作台”按钮显式定位会话。

胶囊的锚点位置、展开方向和拖动区域由资源页面管理，通过 `resize` / `move` 调整窗口尺寸和位置。
