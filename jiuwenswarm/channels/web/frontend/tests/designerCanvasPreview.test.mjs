import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildDesignerBootstrapPreviewGraph,
  isDesignerPreviewGraph,
} from '../node_modules/.cache/designer-canvas-preview/designerBootstrapGraph.js';
import {
  parseMarkdownTable,
  storyboardShotPreviews,
} from '../node_modules/.cache/designer-canvas-preview/designerNodePreview.js';
import { resolveDesignerGraphToLoad } from '../node_modules/.cache/designer-canvas-preview/designerGraphLoad.js';

test('preview graph keeps the default canvas skeleton', () => {
  const graph = buildDesignerBootstrapPreviewGraph('火车进站');
  assert.equal(isDesignerPreviewGraph(graph), true);
  assert.deepEqual(
    graph.nodes.map((node) => node.id),
    ['n_brief', 'n_character', 'n_storyboard', 'n_frame_1', 'n_clip_1', 'n_compose'],
  );
  assert.equal(graph.nodes.find((node) => node.id === 'n_character')?.layout?.y, 140);
  assert.equal(graph.nodes.find((node) => node.id === 'n_storyboard')?.layout?.y, 340);
});

test('storyboardShotPreviews shows action and picture from the Brief columns', () => {
  const shots = storyboardShotPreviews(
    [
      '| Shot | Timeline | Camera | Move | Character action | Scene change | Comment |',
      '| --- | --- | --- | --- | --- | --- | --- |',
      '| 1 | 0.0-2.0s | wide | static | steps off the train | platform morning light | Wide shot of a young man leaving the train |',
      '| 2 | 2.0-5.0s | medium | pan | walks toward the exit | same station | Medium shot walking through the concourse |',
    ].join('\n'),
  );
  assert.equal(shots.length, 2);
  assert.equal(shots[0].shotNo, '1');
  assert.equal(shots[0].action, 'steps off the train');
  assert.equal(shots[0].picture, 'Wide shot of a young man leaving the train');
  assert.equal(shots[1].action, 'walks toward the exit');
});

test('storyboardShotPreviews still lists shots when Comment is empty', () => {
  const shots = storyboardShotPreviews(
    [
      '| Shot | Timeline | Camera | Move | Character action | Scene change | Comment |',
      '| --- | --- | --- | --- | --- | --- | --- |',
      '| 1 | 0.0-2.0s | wide / eye-level | slow pan | steps off the train | platform morning light | |',
    ].join('\n'),
  );
  assert.equal(shots.length, 1);
  assert.equal(shots[0].action, 'steps off the train');
  assert.equal(shots[0].picture, 'platform morning light');
});

test('parseMarkdownTable keeps a table frame from generated markdown', () => {
  const table = parseMarkdownTable(
    [
      '| 镜号 | 画面 | 时长 |',
      '| --- | --- | --- |',
      '| 1 | 火车进站 | 2.0s |',
      '| 2 | 年轻人下车 | 3.0s |',
    ].join('\n'),
  );
  assert.ok(table);
  assert.deepEqual(table.headers, ['镜号', '画面', '时长']);
  assert.equal(table.rows.length, 2);
  assert.equal(table.rows[1][1], '年轻人下车');
});

test('resolveDesignerGraphToLoad keeps the selected graph instead of the first listed one', () => {
  assert.equal(
    resolveDesignerGraphToLoad({
      currentId: 'graph_second',
      listedIds: ['graph_first', 'graph_second'],
    }),
    'graph_second',
  );
  assert.equal(
    resolveDesignerGraphToLoad({
      currentId: 'graph_other_project',
      listedIds: ['graph_first'],
    }),
    'graph_other_project',
  );
  assert.equal(
    resolveDesignerGraphToLoad({
      currentId: 'preview_bootstrap',
      isPreview: true,
      listedIds: ['graph_first'],
    }),
    'graph_first',
  );
  assert.equal(
    resolveDesignerGraphToLoad({
      currentId: '',
      listedIds: ['graph_first', 'graph_second'],
    }),
    'graph_first',
  );
});
