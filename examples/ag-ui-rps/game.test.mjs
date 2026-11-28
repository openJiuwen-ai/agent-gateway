import test from "node:test";
import assert from "node:assert/strict";
import { MOVES, applyTool, beginRound, createGame, playRound, readEvents } from "./game.mjs";

const call = (move, round = 1, id = crypto.randomUUID()) => ({
  id, function: { name: "play_rps", arguments: JSON.stringify({ move, round }) },
});
const eventStream = events => events.map(event => `data: ${JSON.stringify(event)}\r\n\r\n`).join("");
// One-byte chunks deliberately split Chinese UTF-8 characters and CRLF boundaries.
function stream(text) {
  const bytes = new TextEncoder().encode(text);
  return new ReadableStream({ start(controller) {
    for (const byte of bytes) controller.enqueue(Uint8Array.of(byte));
    controller.close();
  } });
}
function response(events) {
  return new Response(stream(eventStream(events)), { headers: { "Content-Type": "text/event-stream" } });
}
function toolEvents(request, calls = [call("scissors")]) {
  return [
    ...calls.flatMap(tool => [
      { type: "TOOL_CALL_START", toolCallId: tool.id, toolCallName: tool.function.name, parentMessageId: request.runId },
      { type: "TOOL_CALL_ARGS", toolCallId: tool.id, delta: tool.function.arguments },
      { type: "TOOL_CALL_END", toolCallId: tool.id },
    ]),
    { type: "RUN_FINISHED", runId: request.runId, threadId: request.threadId,
      outcome: { type: "success", pendingToolCallIds: calls.map(tool => tool.id) } },
  ];
}
function textEvents(request) {
  return [
    { type: "TEXT_MESSAGE_START", messageId: request.runId },
    { type: "TEXT_MESSAGE_CONTENT", messageId: request.runId, delta: "你赢了，下回合见！" },
    { type: "RUN_FINISHED", runId: request.runId, threadId: request.threadId,
      outcome: { type: "success", pendingToolCallIds: [] } },
  ];
}

test("App judges all nine move combinations", () => {
  const expected = { rock: ["draw", "ai", "player"], paper: ["player", "draw", "ai"], scissors: ["ai", "player", "draw"] };
  for (const player of Object.keys(MOVES)) {
    Object.keys(MOVES).forEach((ai, index) => {
      const game = createGame();
      beginRound(game, player);
      const result = JSON.parse(applyTool(game, call(ai)));
      assert.equal(result.outcome, expected[player][index]);
      assert.equal(game.state.scores[result.outcome], 1);
      assert.equal(game.state.history.length, 1);
    });
  }
});

test("all three pending choices expose identical model context", () => {
  const contexts = Object.keys(MOVES).map(move => {
    const game = createGame();
    beginRound(game, move);
    assert.equal(game.pendingMove, move);
    return { state: game.state, prompt: game.messages[0].content };
  });
  assert.deepEqual(contexts[0], contexts[1]);
  assert.deepEqual(contexts[1], contexts[2]);
});

test("App rejects invalid calls and prevents repeat scoring", () => {
  const game = createGame();
  beginRound(game, "rock");
  const invalid = [call("lizard"), call("paper", 2), call("paper", "1"),
    { id: "bad-json", function: { name: "play_rps", arguments: "{" } },
    { id: "null", function: { name: "play_rps", arguments: "null" } },
    { id: "extra", function: { name: "play_rps", arguments: '{"move":"paper","round":1,"score":99}' } },
    { id: "other-tool", function: { name: "set_score", arguments: '{"move":"paper","round":1}' } }];
  for (const tool of invalid) assert.equal(JSON.parse(applyTool(game, tool)).status, "rejected");
  assert.equal(game.state.history.length, 0);
  const valid = call("scissors");
  const first = applyTool(game, valid);
  assert.equal(applyTool(game, valid), first);
  assert.equal(JSON.parse(applyTool(game, call("paper"))).status, "rejected");
  assert.deepEqual(game.state.scores, { player: 1, ai: 0, draw: 0 });
  beginRound(game, "paper");
  assert.equal(applyTool(game, valid), first);
  assert.equal(game.state.status, "awaiting_ai");
});

test("SSE handles fragmented Unicode, comments and multi-line data", async () => {
  const events = [];
  for await (const event of readEvents(stream(': keepalive\r\ndata: {"type":"TEXT_MESSAGE_CONTENT",\r\ndata: "delta":"石头"}\r\n\r\n'))) events.push(event);
  assert.deepEqual(events, [{ type: "TEXT_MESSAGE_CONTENT", delta: "石头" }]);
  await assert.rejects(async () => {
    for await (const event of readEvents(stream('data: {"type":"RUN_FINISHED"}'))) void event;
  }, /不完整/);
});

test("full rounds hide pending moves and submit tool results separately with latest state", async () => {
  const game = createGame();
  const requests = [];
  let text = "";
  const fetchImpl = async (url, options) => {
    assert.equal(url, "http://gateway/api/ag-ui/run");
    assert.equal(options.headers.Authorization, "Bearer demo");
    const request = JSON.parse(options.body);
    requests.push(request);
    if (request.messages.at(-1).role === "user") {
      assert.equal(request.state.status, "awaiting_ai");
      assert.equal(Object.hasOwn(request.state, "pendingMove"), false);
      assert.equal(JSON.stringify(request).includes('"player":"paper"'), false);
      return response(toolEvents(request, [call("scissors", request.state.round)]));
    }
    assert.equal(request.messages.at(-1).role, "tool");
    assert.equal(request.state.status, "resolved");
    assert.equal(request.state.history.at(-1).player, request.state.round === 1 ? "rock" : "paper");
    return response(textEvents(request));
  };
  beginRound(game, "rock");
  await playRound(game, { endpoint: "http://gateway/api/ag-ui/run", token: "demo", fetchImpl, onText: delta => { text += delta; } });
  beginRound(game, "paper");
  await playRound(game, { endpoint: "http://gateway/api/ag-ui/run", token: "demo", fetchImpl });
  assert.equal(text, "你赢了，下回合见！");
  assert.equal(requests.length, 4);
  assert.equal(new Set(requests.map(r => r.runId)).size, 4);
  assert.equal(new Set(requests.map(r => r.threadId)).size, 1);
  assert.deepEqual(requests[1].messages.slice(0, requests[0].messages.length), requests[0].messages);
  assert.deepEqual(requests[2].messages.slice(0, requests[1].messages.length), requests[1].messages);
  assert.deepEqual(game.state.scores, { player: 1, ai: 1, draw: 0 });
});

test("batch submits both success and business rejection without changing score twice", async () => {
  const game = createGame();
  beginRound(game, "rock");
  let runs = 0;
  await playRound(game, { endpoint: "http://gateway", fetchImpl: async (_, options) => {
    const request = JSON.parse(options.body);
    if (++runs === 1) return response(toolEvents(request, [call("scissors"), call("paper")]));
    assert.deepEqual(request.messages.slice(-2).map(m => JSON.parse(m.content).status), ["success", "rejected"]);
    return response(textEvents(request));
  } });
  assert.equal(game.state.history.length, 1);
  assert.equal(runs, 2);
});

test("incomplete, failed and inconsistent runs never execute their tool calls", async () => {
  for (const kind of ["disconnect", "error", "mismatch", "unfinished-tool"]) {
    const game = createGame();
    beginRound(game, "rock");
    let requests = 0;
    await assert.rejects(playRound(game, { endpoint: "http://gateway", fetchImpl: async (_, options) => {
      requests += 1;
      const events = toolEvents(JSON.parse(options.body));
      if (kind === "disconnect") events.pop();
      if (kind === "error") events[events.length - 1] = { type: "RUN_ERROR", message: "failed" };
      if (kind === "mismatch") events.at(-1).outcome.pendingToolCallIds = ["unknown"];
      if (kind === "unfinished-tool") events.splice(-2, 1);
      return response(events);
    } }));
    assert.equal(requests, 1);
    assert.equal(game.state.history.length, 0);
    assert.equal(game.pendingMove, "rock");
  }
});
