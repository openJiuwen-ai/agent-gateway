import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  fromReactFlowGraph,
  toReactFlowGraph,
} from '../node_modules/.cache/designer-graph-adapter/designerGraphAdapter.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const fixturePath = path.join(__dirname, 'fixtures', 'designer-execution-graph.v1.json');
const fixture = JSON.parse(readFileSync(fixturePath, 'utf8'));

test('toReactFlowGraph maps domain nodes and edges', () => {
  const view = toReactFlowGraph(fixture);
  assert.equal(view.nodes.length, fixture.nodes.length);
  assert.equal(view.edges.length, fixture.edges.length);
  const brief = view.nodes.find((node) => node.id === 'n_brief');
  assert.ok(brief);
  assert.deepEqual(brief.position, { x: 40, y: 200 });
  assert.equal(brief.type, 'text');
  assert.equal(brief.data.label, '项目 brief');
});

test('fromReactFlowGraph preserves domain semantics while updating layout', () => {
  const view = toReactFlowGraph(fixture);
  const moved = {
    ...view,
    nodes: view.nodes.map((node) =>
      node.id === 'n_brief'
        ? { ...node, position: { x: 100, y: 200 } }
        : node,
    ),
  };
  const merged = fromReactFlowGraph(moved, fixture);
  const brief = merged.nodes.find((node) => node.id === 'n_brief');
  assert.ok(brief);
  assert.equal(brief.layout?.x, 100);
  assert.equal(brief.layout?.y, 200);
  assert.equal(brief.type, 'text');
  assert.equal(
    merged.edges.find((edge) => edge.id === 'e_frame_1_clip_1')?.source,
    'n_frame_1',
  );
  assert.equal(
    merged.edges.find((edge) => edge.id === 'e_clip_1_final')?.target,
    'n_final',
  );
  assert.equal(
    merged.edges.find((edge) => edge.id === 'e_character_storyboard'),
    undefined,
  );
});
