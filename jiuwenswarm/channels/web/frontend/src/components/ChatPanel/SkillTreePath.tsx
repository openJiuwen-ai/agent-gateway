/**
 * SkillTreePath 组件
 *
 * 当 agentic search（symphony 渐进检索）在对话中被触发时，把后端下发的
 * 「技能树路径」按遍历顺序逐级回放展示，营造路径流转效果，最终落到命中的技能。
 *
 * 数据来源：skill_retrieve 工具结果 raw_output.skill_tree（见 types/skillTree.ts）。
 * 纯 SVG/DOM 实现，不引入第三方图表库。
 */

import { useEffect, useMemo, useRef, useState } from 'react';
import clsx from 'clsx';
import type { SkillTreePath as SkillTreePathData, SkillTreeStep } from '../../types/skillTree';
import './SkillTreePath.css';

interface SkillTreePathProps {
  tree: SkillTreePathData;
  /** 流转动画每步间隔（ms），设 0 关闭动画直接全量展示 */
  stepIntervalMs?: number;
}

type StepKind = 'explore' | 'select' | 'descend' | 'complete';

interface FlowRow {
  key: string;
  step: SkillTreeStep;
  kind: StepKind;
}

function classifyStep(step: SkillTreeStep): StepKind | null {
  switch (step.event_type) {
    case 'fragment_built':
      return 'explore';
    case 'fragment_selected':
      return 'select';
    case 'fragment_continue':
      return 'descend';
    case 'search_complete':
      return 'complete';
    // reduce_complete 等内部归并事件不展示，避免噪声
    default:
      return null;
  }
}

function buildRows(steps: SkillTreeStep[]): FlowRow[] {
  const rows: FlowRow[] = [];
  for (const step of steps) {
    const kind = classifyStep(step);
    if (!kind) continue;
    // 探查节点若只有 0/1 个可选项，信息量低，跳过以突出真正的分支决策
    if (kind === 'explore' && (step.selectable_count ?? 0) <= 1 && step.selected.length === 0) {
      continue;
    }
    rows.push({ key: `${step.order}-${step.event_type}`, step, kind });
  }
  return rows;
}

const KIND_ICON: Record<StepKind, JSX.Element> = {
  explore: (
    <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4" aria-hidden="true">
      <circle cx="7" cy="7" r="4.2" />
      <path strokeLinecap="round" d="M10.2 10.2 13 13" />
    </svg>
  ),
  select: (
    <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4" aria-hidden="true">
      <path strokeLinecap="round" strokeLinejoin="round" d="M3.5 8.2 6.4 11l6.1-6.4" />
    </svg>
  ),
  descend: (
    <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4" aria-hidden="true">
      <path strokeLinecap="round" strokeLinejoin="round" d="M8 3v8.4M4.8 8.6 8 11.8l3.2-3.2" />
    </svg>
  ),
  complete: (
    <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4" aria-hidden="true">
      <circle cx="8" cy="8" r="5.2" />
      <path strokeLinecap="round" strokeLinejoin="round" d="M5.6 8.1 7.3 9.8l3.3-3.6" />
    </svg>
  ),
};

function StepRow({ row, revealed }: { row: FlowRow; revealed: boolean }) {
  const { step, kind } = row;
  const indent = Math.min(step.depth, 6) * 16;

  return (
    <div
      className={clsx('skill-path__row', `skill-path__row--${kind}`, revealed && 'is-revealed')}
      style={{ marginLeft: indent }}
      data-depth={step.depth}
    >
      <span className="skill-path__rail" aria-hidden="true" />
      <span className={clsx('skill-path__icon', `skill-path__icon--${kind}`)}>{KIND_ICON[kind]}</span>
      <span className="skill-path__body">
        <span className="skill-path__line">
          <span className="skill-path__depth">Lv.{step.depth}</span>
          <span className="skill-path__text">{stepText(row)}</span>
        </span>
        {step.selected.length > 0 && (
          <span className="skill-path__chips">
            {step.selected.map((node) => (
              <span key={node.id} className="skill-path__chip" title={node.id}>
                {node.label || node.id}
              </span>
            ))}
          </span>
        )}
        {step.leaves.length > 0 && (
          <span className="skill-path__chips">
            {step.leaves.map((leaf) => (
              <span key={leaf.id} className="skill-path__chip skill-path__chip--leaf" title={leaf.id}>
                {leaf.label || leaf.id}
              </span>
            ))}
          </span>
        )}
      </span>
    </div>
  );
}

function stepText(row: FlowRow): string {
  const { step, kind } = row;
  const name = step.label || step.node_id || '根节点';
  switch (kind) {
    case 'explore':
      return step.selectable_count != null
        ? `探查「${name}」 · ${step.selectable_count} 个候选分支`
        : `探查「${name}」`;
    case 'select':
      return `在「${name}」选择分支`;
    case 'descend':
      return step.leaves.length > 0 ? '下钻并命中技能叶子' : '下钻到子分支';
    case 'complete':
      return `检索完成 · 命中 ${step.candidate_count ?? 0} 个技能`;
    default:
      return name;
  }
}

export function SkillTreePath({ tree, stepIntervalMs = 320 }: SkillTreePathProps) {
  const rows = useMemo(() => buildRows(tree.steps), [tree.steps]);
  const [revealCount, setRevealCount] = useState(stepIntervalMs > 0 ? 0 : rows.length);
  const [collapsed, setCollapsed] = useState(false);
  const timerRef = useRef<number | null>(null);

  useEffect(() => {
    if (stepIntervalMs <= 0) {
      setRevealCount(rows.length);
      return;
    }
    setRevealCount(0);
    if (rows.length === 0) return;
    timerRef.current = window.setInterval(() => {
      setRevealCount((current) => {
        if (current >= rows.length) {
          if (timerRef.current != null) {
            window.clearInterval(timerRef.current);
            timerRef.current = null;
          }
          return current;
        }
        return current + 1;
      });
    }, stepIntervalMs);
    return () => {
      if (timerRef.current != null) {
        window.clearInterval(timerRef.current);
        timerRef.current = null;
      }
    };
  }, [rows.length, stepIntervalMs]);

  const allRevealed = revealCount >= rows.length;
  const candidates = tree.candidates ?? [];

  return (
    <div className="skill-path animate-rise" data-testid="skill-tree-path">
      <button
        type="button"
        className="skill-path__header"
        onClick={() => setCollapsed((value) => !value)}
        aria-expanded={!collapsed}
      >
        <span className="skill-path__title">
          <span className="skill-path__badge">技能树路径</span>
          {tree.query && <span className="skill-path__query" title={tree.query}>{tree.query}</span>}
        </span>
        <span className="skill-path__meta">
          {!allRevealed && <span className="skill-path__live">流转中…</span>}
          <span>{tree.candidate_count} 个技能</span>
          {tree.elapsed_ms != null && <span>{Math.round(tree.elapsed_ms)}ms</span>}
          <span className={clsx('skill-path__chevron', !collapsed && 'is-open')} aria-hidden="true">
            <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.8">
              <path strokeLinecap="round" strokeLinejoin="round" d="m6.5 8 3.5 4 3.5-4" />
            </svg>
          </span>
        </span>
      </button>

      {!collapsed && (
        <div className="skill-path__body-wrap">
          <div className="skill-path__flow">
            {rows.length === 0 ? (
              <div className="skill-path__empty">未记录到路径遍历步骤。</div>
            ) : (
              rows.map((row, index) => (
                <StepRow key={row.key} row={row} revealed={index < revealCount} />
              ))
            )}
          </div>

          {candidates.length > 0 && (
            <div className={clsx('skill-path__results', allRevealed && 'is-revealed')}>
              <div className="skill-path__results-title">命中技能</div>
              <ul className="skill-path__results-list">
                {candidates.map((candidate) => (
                  <li
                    key={`${candidate.rank}-${candidate.worker_id || candidate.label}`}
                    className={clsx('skill-path__result', candidate.selected && 'is-top')}
                  >
                    <span className="skill-path__result-rank">#{candidate.rank}</span>
                    <span className="skill-path__result-main">
                      <span className="skill-path__result-name" title={candidate.worker_id}>
                        {candidate.label}
                      </span>
                      {candidate.description && (
                        <span className="skill-path__result-desc">{candidate.description}</span>
                      )}
                    </span>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
