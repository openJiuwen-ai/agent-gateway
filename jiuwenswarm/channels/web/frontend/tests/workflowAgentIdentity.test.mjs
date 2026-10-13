import assert from 'node:assert/strict';
import test from 'node:test';

import {
  workflowAgentAvatarName,
  workflowAgentDisplayName,
  workflowAgentIdentity,
} from '../node_modules/.cache/workflow-agent-identity/workflowTypes.js';

test('third-party node resolves to its CLI brand identity', () => {
  assert.deepEqual(workflowAgentIdentity({ agent_type: 'codex', kind: 'agent' }), { cliAgent: 'codex' });
  assert.deepEqual(workflowAgentIdentity({ agent_type: 'claude' }), { cliAgent: 'claude' });
});

test('human node resolves to the human_agent role', () => {
  assert.deepEqual(workflowAgentIdentity({ kind: 'human' }), { role: 'human_agent' });
});

test('native node gets an empty identity (hashed illustration, no roster fallback)', () => {
  assert.deepEqual(workflowAgentIdentity({ kind: 'agent' }), {});
  assert.deepEqual(workflowAgentIdentity({}), {});
});

test('agent_type wins over kind', () => {
  assert.deepEqual(workflowAgentIdentity({ agent_type: 'codex', kind: 'human' }), { cliAgent: 'codex' });
});

test('expert node shows and hashes by its display name, plain node by its label', () => {
  assert.equal(workflowAgentDisplayName({ name: 'clip_3', display_name: '视频镜头师' }), '视频镜头师');
  assert.equal(workflowAgentAvatarName({ name: 'clip_3', display_name: '视频镜头师' }), '视频镜头师');
  assert.equal(workflowAgentDisplayName({ name: 'free' }), 'free');
  assert.equal(workflowAgentAvatarName({ name: 'free' }), 'free');
});

test('expert node keeps the native identity (no CLI brand, no human role)', () => {
  assert.deepEqual(workflowAgentIdentity({ agent_type: undefined, kind: 'agent', agent_template: 'clip' }), {});
});
