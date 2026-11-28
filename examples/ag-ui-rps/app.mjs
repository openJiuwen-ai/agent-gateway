import { MOVES, createGame, beginRound, playRound } from "./game.mjs";

const $ = id => document.getElementById(id);
const hands = { rock: "✊", paper: "🖐️", scissors: "✌️" };
const outcomes = { player: "你赢了！", ai: "AI 赢了", draw: "平局，再来！" };
const choices = [...document.querySelectorAll("[data-move]")];
let game = createGame();
let busy = false;
let failed = false;

$("origin-hint").textContent = `当前页面 Origin：${location.origin}。请在 Gateway 进程设置 AG_UI_ALLOWED_ORIGINS 包含此地址，并重启 Gateway。`;

function render() {
  for (const side of ["player", "ai", "draw"]) $(`${side}-score`).textContent = game.state.scores[side];
  for (const button of choices) button.disabled = busy || failed;
  $("reset").disabled = busy;
  $("endpoint").disabled = busy || game.messages.length > 0;
  $("token").disabled = busy || game.messages.length > 0;
  $("empty-history").hidden = game.state.history.length > 0;
  $("history").replaceChildren(...game.state.history.slice().reverse().map(round => {
    const row = document.createElement("li");
    for (const text of [`#${String(round.round).padStart(2, "0")}`, `你 ${hands[round.player]} ${MOVES[round.player]}`, `AI ${hands[round.ai]} ${MOVES[round.ai]}`, outcomes[round.outcome]]) {
      const cell = document.createElement("span");
      cell.textContent = text;
      row.append(cell);
    }
    row.dataset.outcome = round.outcome;
    return row;
  }));
  const last = game.state.history.at(-1);
  if (last && game.state.status === "resolved") {
    $("player-hand").textContent = hands[last.player];
    $("ai-hand").textContent = hands[last.ai];
    $("player-move").textContent = MOVES[last.player];
    $("ai-move").textContent = MOVES[last.ai];
    $("status").textContent = `第 ${last.round} 回合 · ${outcomes[last.outcome]}${busy ? " 正在等待 AI 点评…" : " 选个手势继续"}`;
  }
}

for (const button of choices) button.addEventListener("click", async () => {
  if (busy || failed) return;
  let endpoint;
  try {
    endpoint = new URL($("endpoint").value.trim());
    if (!["http:", "https:"].includes(endpoint.protocol)) throw new Error();
  } catch {
    $("status").textContent = "请填写有效的 HTTP(S) Gateway 地址";
    $("endpoint").focus();
    return;
  }
  beginRound(game, button.dataset.move);
  busy = true;
  $("player-hand").textContent = hands[button.dataset.move];
  $("player-move").textContent = "已锁定";
  $("ai-hand").textContent = "❔";
  $("ai-move").textContent = "思考中";
  $("status").textContent = `第 ${game.state.round} 回合 · 等待 AI 出拳…`;
  $("commentary").textContent = "";
  render();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 660_000);
  let lastError = null;
  try {
    await playRound(game, {
      endpoint: endpoint.href, token: $("token").value.trim(), signal: controller.signal,
      onText: delta => { $("commentary").textContent += delta; },
      onState: render,
    });
    document.querySelector(".connection").open = false;
    if (!$("commentary").textContent) $("commentary").textContent = "这一回合结束了，再来一局！";
  } catch (error) {
    failed = true;
    if (error.name === "AbortError") {
      lastError = "等待超时";
    } else if (error instanceof TypeError) {
      lastError = `无法连接 ${endpoint.href}，或请求被浏览器跨域策略拦截。确认 Gateway 的 HTTP 端口，并在 Gateway 进程设置 AG_UI_ALLOWED_ORIGINS 包含 ${location.origin} 后重启`;
    } else {
      lastError = error.message;
    }
    document.querySelector(".connection").open = true;
  } finally {
    clearTimeout(timeout);
    busy = false;
    render();
    if (lastError) $("status").textContent = `${lastError}。请检查地址、Origin 白名单和宿主能力，然后点“新开一局”。`;
  }
});

$("reset").addEventListener("click", () => {
  if (busy) return;
  game = createGame();
  failed = false;
  for (const side of ["player", "ai"]) $(`${side}-hand`).textContent = "✊";
  $("player-move").textContent = "准备出拳";
  $("ai-move").textContent = "等你挑战";
  $("status").textContent = "选择你的手势，开启第 1 回合";
  $("commentary").textContent = "新的对局，看看这次谁能猜中。";
  render();
});
