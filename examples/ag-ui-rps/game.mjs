// The application owns the rules and score. The model only chooses its move.
export const MOVES = Object.freeze({ rock: "石头", paper: "布", scissors: "剪刀" });
const BEATS = { rock: "scissors", paper: "rock", scissors: "paper" };
const isMove = value => Object.hasOwn(MOVES, value);

export const TOOL = {
  name: "play_rps",
  description: "作为玩家的猜拳对手，为当前回合出拳。玩家已在浏览器锁定选择，揭晓前你无法看到。每回合只能成功出拳一次；由 App 判定结果和更新比分。",
  parameters: {
    type: "object",
    properties: {
      round: { type: "integer", minimum: 1, description: "当前 state.round" },
      move: { type: "string", enum: Object.keys(MOVES) },
    },
    required: ["round", "move"],
    additionalProperties: false,
  },
};

export function createGame() {
  return {
    threadId: crypto.randomUUID(), messages: [], executed: new Map(), pendingMove: null,
    state: { round: 0, status: "ready", scores: { player: 0, ai: 0, draw: 0 }, history: [] },
  };
}

export function beginRound(game, move) {
  if (!isMove(move) || game.pendingMove !== null) throw new Error("无法开始这一回合");
  game.pendingMove = move;
  game.state.round += 1;
  game.state.status = "awaiting_ai";
  // Never put the player's pending move into messages, state, or context.
  game.messages.push({
    id: crypto.randomUUID(), role: "user",
    content: `开始第 ${game.state.round} 回合猜拳。我已在本地锁定出拳，请调用 play_rps 选择你的出拳。工具返回后用中文简短点评，等待下一回合，不要再次出拳。`,
  });
}

export function applyTool(game, call) {
  if (game.executed.has(call.id)) return game.executed.get(call.id);
  let result;
  try {
    const args = JSON.parse(call.function.arguments);
    if (call.function.name !== TOOL.name || !args || Array.isArray(args)
        || Object.keys(args).sort().join(",") !== "move,round"
        || !Number.isInteger(args.round) || args.round !== game.state.round || !isMove(args.move)) {
      throw new Error("工具名、回合或出拳参数无效");
    }
    if (game.state.status !== "awaiting_ai" || game.pendingMove === null) {
      throw new Error("本回合已结算，等待玩家开始下一回合");
    }
    const player = game.pendingMove;
    const outcome = player === args.move ? "draw" : BEATS[player] === args.move ? "player" : "ai";
    const round = { round: args.round, player, ai: args.move, outcome };
    game.state.scores[outcome] += 1;
    game.state.history.push(round);
    game.state.status = "resolved";
    game.pendingMove = null;
    result = { status: "success", ...round, scores: { ...game.state.scores } };
  } catch (error) {
    result = { status: "rejected", error: error.message };
  }
  const encoded = JSON.stringify(result);
  game.executed.set(call.id, encoded);
  return encoded;
}

// Decode SSE frames, including UTF-8 and CRLF split across network chunks.
export async function* readEvents(body) {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let data = [];
  function line(value) {
    if (value === "") {
      const payload = data.length ? JSON.parse(data.join("\n")) : null;
      data = [];
      return payload;
    }
    if (value.startsWith("data:")) data.push(value.slice(5).replace(/^ /, ""));
    return null;
  }
  try {
    while (true) {
      const { value, done } = await reader.read();
      buffer += done ? decoder.decode() : decoder.decode(value, { stream: true });
      let newline;
      while ((newline = buffer.indexOf("\n")) !== -1) {
        const event = line(buffer.slice(0, newline).replace(/\r$/, ""));
        buffer = buffer.slice(newline + 1);
        if (event) yield event;
      }
      if (done) {
        // A partial final frame cannot prove a successful execution.
        if (buffer.trim() || data.length) throw new Error("SSE 数据帧不完整");
        return;
      }
    }
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

export async function playRound(game, { endpoint, token = "", signal, onText = () => {}, onState = () => {}, fetchImpl = fetch }) {
  for (let step = 0; step < 8; step += 1) {
    const runId = crypto.randomUUID();
    const headers = { "Content-Type": "application/json", Accept: "text/event-stream" };
    if (token) headers.Authorization = `Bearer ${token}`;
    const response = await fetchImpl(endpoint, {
      method: "POST", headers, signal,
      body: JSON.stringify({
        threadId: game.threadId, runId, protocolVersion: "1.0",
        messages: game.messages, tools: [TOOL], state: game.state,
        context: [{ description: "猜拳规则", value: "你是玩家的对手。石头胜剪刀、剪刀胜布、布胜石头，相同为平局。用 play_rps 出拳；只信任工具返回的判分，不要修改比分。当前玩家的选择仅存于浏览器本地。" }],
      }),
    });
    if (!response.ok) {
      const detail = await response.text();
      throw new Error(`HTTP ${response.status}: ${detail.slice(0, 400)}`);
    }
    if (!response.body || !response.headers.get("content-type")?.includes("text/event-stream")) {
      throw new Error("服务端没有返回 SSE 流，请检查 Gateway 地址");
    }
    let assistant = null;
    let finished = null;
    const calls = new Map();
    const ended = new Set();
    function message(id) {
      if (!id) throw new Error("助手消息缺少 ID");
      if (!assistant) assistant = { id, role: "assistant", content: "" };
      if (assistant.id !== id) throw new Error("同一 Run 返回了不同助手消息");
      return assistant;
    }
    for await (const event of readEvents(response.body)) {
      if (finished) throw new Error("RUN_FINISHED 后出现额外事件");
      switch (event.type) {
        case "TEXT_MESSAGE_START": message(event.messageId); break;
        case "TEXT_MESSAGE_CONTENT":
          message(event.messageId).content += event.delta;
          onText(event.delta);
          break;
        case "TOOL_CALL_START": {
          if (!event.toolCallId || calls.has(event.toolCallId)) throw new Error("工具调用 ID 无效或重复");
          const call = { id: event.toolCallId, type: "function", function: { name: event.toolCallName, arguments: "" } };
          calls.set(call.id, call);
          (message(event.parentMessageId).toolCalls ??= []).push(call);
          break;
        }
        case "TOOL_CALL_ARGS":
          if (!calls.has(event.toolCallId) || ended.has(event.toolCallId)) throw new Error("工具参数顺序无效");
          calls.get(event.toolCallId).function.arguments += event.delta;
          break;
        case "TOOL_CALL_END":
          if (!calls.has(event.toolCallId) || ended.has(event.toolCallId)) throw new Error("工具结束事件无效");
          ended.add(event.toolCallId);
          break;
        case "RUN_ERROR": throw new Error(`${event.code ?? "RUN_ERROR"}: ${event.message}`);
        case "RUN_FINISHED":
          if (event.runId !== runId || event.threadId !== game.threadId) throw new Error("Run 标识不匹配");
          finished = event;
          break;
        // State events cannot overwrite the application-owned score or reveal a move.
      }
    }
    if (!finished || !assistant) throw new Error("运行未完整结束，请新开一局，不要重放请求");
    const pending = finished.outcome?.pendingToolCallIds;
    if (finished.outcome?.type !== "success" || !Array.isArray(pending) || new Set(pending).size !== pending.length
        || pending.length !== calls.size || ended.size !== calls.size
        || pending.some(id => !calls.has(id))) {
      throw new Error("工具批次与 Run 结束状态不一致");
    }
    game.messages.push(assistant);
    if (!pending.length) {
      if (game.state.status !== "resolved") throw new Error("AI 未调用出拳工具，请新开一局重试");
      return;
    }
    // Execute only after the entire Run succeeds, then submit all results together.
    for (const id of pending) {
      const content = applyTool(game, calls.get(id));
      game.messages.push({ id: crypto.randomUUID(), role: "tool", toolCallId: id, content });
    }
    onState();
    // The next request contains only new tool messages, never a new user message.
  }
  throw new Error("工具调用轮数过多，请新开一局");
}
