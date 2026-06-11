import {
  Children,
  isValidElement,
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
  type AnchorHTMLAttributes,
  type HTMLAttributes,
  type ReactNode,
} from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { useTranslation } from 'react-i18next';
import clsx from 'clsx';
import type { MermaidConfig } from 'mermaid';
import type { Element as HastElement } from 'hast';
import {
  Copy,
  Check,
  ZoomIn,
  ZoomOut,
  RotateCcw,
} from 'lucide-react';

interface MarkdownRendererProps {
  content: string;
  className?: string;
  testId?: string;
}

type MermaidRenderState =
  | { status: 'loading'; svg: '' }
  | { status: 'rendered'; svg: string }
  | { status: 'error'; svg: '' };

const MERMAID_CONFIG: MermaidConfig = {
  startOnLoad: false,
  suppressErrorRendering: true,
  securityLevel: 'strict',
  htmlLabels: false,
};

function getMermaidTheme(): 'default' | 'dark' {
  return document.documentElement.getAttribute('data-theme') === 'light'
    ? 'default'
    : 'dark';
}

function ToolbarButton({
  title,
  onClick,
  children,
}: {
  title: string;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      title={title}
      onClick={onClick}
      className="inline-flex items-center justify-center w-8 h-8 rounded-md border border-transparent bg-transparent text-muted hover:text-text hover:bg-bg-hover hover:border-border transition-colors duration-fast cursor-pointer flex-shrink-0"
    >
      {children}
    </button>
  );
}

function TogglePill({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={clsx(
        'inline-flex items-center gap-1.5 px-3 py-1 rounded-md text-xs font-medium transition-all cursor-pointer',
        active
          ? 'bg-bg-elevated text-text shadow-sm border border-border'
          : 'text-muted hover:text-text border border-transparent'
      )}
    >
      {children}
    </button>
  );
}

function clampScale(s: number): number {
  return Math.min(Math.max(s, 0.25), 3);
}

function MermaidBlock({ code }: { code: string }) {
  const { t } = useTranslation();
  const diagramId = `mermaid-${useId().replace(/[^A-Za-z0-9_-]/g, '_')}`;
  const [renderState, setRenderState] = useState<MermaidRenderState>({
    status: 'loading',
    svg: '',
  });
  const [viewMode, setViewMode] = useState<'image' | 'code'>('image');
  const [scale, setScale] = useState(1);
  const [copied, setCopied] = useState(false);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  const [isDragging, setIsDragging] = useState(false);
  const isDraggingRef = useRef(false);
  const dragStartRef = useRef({ x: 0, y: 0 });
  const panStartRef = useRef({ x: 0, y: 0 });

  useEffect(() => {
    let cancelled = false;
    async function render(): Promise<void> {
      setRenderState({ status: 'loading', svg: '' });
      try {
        const mermaid = (await import('mermaid')).default;
        mermaid.initialize({ ...MERMAID_CONFIG, theme: getMermaidTheme() });
        const { svg } = await mermaid.render(diagramId, code);
        if (!cancelled) setRenderState({ status: 'rendered', svg });
      } catch {
        if (!cancelled) setRenderState({ status: 'error', svg: '' });
      }
    }
    render();
    return () => { cancelled = true; };
  }, [code, diagramId]);

  async function handleCopy(): Promise<void> {
    try {
      await navigator.clipboard.writeText(code);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 2000);
    } catch {
      // clipboard may fail silently
    }
  }

  function startDrag(clientX: number, clientY: number): void {
    isDraggingRef.current = true;
    setIsDragging(true);
    dragStartRef.current = { x: clientX, y: clientY };
    panStartRef.current = { ...pan };
  }

  function moveDrag(clientX: number, clientY: number): void {
    if (!isDraggingRef.current) return;
    const dx = clientX - dragStartRef.current.x;
    const dy = clientY - dragStartRef.current.y;
    setPan({ x: panStartRef.current.x + dx, y: panStartRef.current.y + dy });
  }

  function endDrag(): void {
    isDraggingRef.current = false;
    setIsDragging(false);
  }

  if (renderState.status === 'error') {
    return (
      <pre className="mermaid-error" data-mermaid-status="error">
        <code>{code}</code>
      </pre>
    );
  }

  if (renderState.status === 'loading') {
    return (
      <pre className="mermaid-loading" data-mermaid-status="loading">
        <code>{code}</code>
      </pre>
    );
  }

  return (
    <div
      className="mermaid-diagram my-4 rounded-xl border border-border bg-bg-elevated overflow-hidden"
      data-mermaid-status="rendered"
    >
      {/* Toolbar */}
      <div className="mermaid-diagram__toolbar flex items-center justify-between px-3 py-2 border-b border-border bg-bg-accent">
        {/* Left: View toggle */}
        <div className="inline-flex items-center rounded-lg bg-secondary p-0.5 border border-border">
          <TogglePill
            active={viewMode === 'image'}
            onClick={() => setViewMode('image')}
          >
            {t('mermaid.image')}
          </TogglePill>
          <TogglePill
            active={viewMode === 'code'}
            onClick={() => setViewMode('code')}
          >
            {t('mermaid.code')}
          </TogglePill>
        </div>

        {/* Right: Actions */}
        <div className="flex items-center gap-1">
          <ToolbarButton title={t('mermaid.copyCode')} onClick={handleCopy}>
            {copied ? (
              <Check size={15} className="text-ok" />
            ) : (
              <Copy size={15} />
            )}
          </ToolbarButton>
          {viewMode === 'image' && (
            <>
              <div className="w-px h-4 bg-border mx-0.5" />
              <ToolbarButton
                title={t('mermaid.zoomIn')}
                onClick={() => setScale((s) => clampScale(s + 0.25))}
              >
                <ZoomIn size={15} />
              </ToolbarButton>
              <ToolbarButton
                title={t('mermaid.zoomOut')}
                onClick={() => setScale((s) => clampScale(s - 0.25))}
              >
                <ZoomOut size={15} />
              </ToolbarButton>
              <ToolbarButton
                title={t('mermaid.fitView')}
                onClick={() => { setScale(1); setPan({ x: 0, y: 0 }); }}
              >
                <RotateCcw size={15} />
              </ToolbarButton>
            </>
          )}
        </div>
      </div>

      {/* Content area */}
      {viewMode === 'image' ? (
        <div
          className="mermaid-canvas relative overflow-hidden select-none"
          style={{
            height: '600px',
            cursor: isDragging ? 'grabbing' : 'grab',
            touchAction: 'none',
          }}
          onMouseDown={(e) => { e.preventDefault(); startDrag(e.clientX, e.clientY); }}
          onMouseMove={(e) => moveDrag(e.clientX, e.clientY)}
          onMouseUp={endDrag}
          onMouseLeave={endDrag}
          onTouchStart={(e) => { const touch = e.touches[0]; startDrag(touch.clientX, touch.clientY); }}
          onTouchMove={(e) => { const touch = e.touches[0]; moveDrag(touch.clientX, touch.clientY); }}
          onTouchEnd={endDrag}
        >
          <div
            className="mermaid-svg-wrapper absolute left-1/2 top-0"
            style={{
              transform: `translate(-50%, 24px) translate(${pan.x}px, ${pan.y}px) scale(${scale})`,
              transition: isDragging
                ? 'none'
                : 'transform var(--duration-fast) var(--ease-out)',
            }}
            dangerouslySetInnerHTML={{ __html: renderState.svg }}
          />
        </div>
      ) : (
        <div className="relative overflow-auto px-4">
          <pre className="text-sm font-mono text-text whitespace-pre">
            <code>{code}</code>
          </pre>
        </div>
      )}
    </div>
  );
}

function getMermaidCode(children: ReactNode): string | null {
  const childArray = Children.toArray(children);
  if (childArray.length !== 1) {
    return null;
  }

  const child = childArray[0];
  if (!isValidElement<HTMLAttributes<HTMLElement>>(child) || child.type !== 'code') {
    return null;
  }

  const className = child.props.className || '';
  if (!/(^|\s)language-mermaid(\s|$)/.test(className)) {
    return null;
  }

  return String(child.props.children).replace(/\n$/, '');
}

function isCompleteCodeFence(
  contentLines: string[],
  node?: HastElement
): boolean {
  const startLine = node?.position?.start?.line;
  const endLine = node?.position?.end?.line;
  if (!startLine || !endLine) {
    return false;
  }

  const opener = contentLines[startLine - 1];
  const closer = contentLines[endLine - 1];
  if (!opener || !closer) {
    return false;
  }

  const openMatch = /^( {0,3})(`{3,}|~{3,})/.exec(opener);
  if (!openMatch) {
    return false;
  }

  const fenceChar = openMatch[2][0];
  const fenceLen = openMatch[2].length;
  const closePattern = new RegExp(`^ {0,3}\\${fenceChar}{${fenceLen},}\\s*$`);
  return closePattern.test(closer);
}

function MarkdownLink({
  href,
  children,
  ...props
}: AnchorHTMLAttributes<HTMLAnchorElement>) {
  return (
    <a href={href} target="_blank" rel="noopener noreferrer" {...props}>
      {children}
    </a>
  );
}

function MarkdownPre({
  children,
  node,
  contentLines,
  ...props
}: HTMLAttributes<HTMLPreElement> & {
  node?: HastElement;
  contentLines: string[];
}) {
  const code = getMermaidCode(children);
  if (code !== null && isCompleteCodeFence(contentLines, node)) {
    return <MermaidBlock code={code} />;
  }

  return (
    <pre {...props}>
      {children}
    </pre>
  );
}

export function MarkdownRenderer({
  content,
  className,
  testId,
}: MarkdownRendererProps) {
  const contentLines = useMemo(
    () => content.split(/\r\n|\n|\r/),
    [content]
  );

  const components = useMemo(
    () => ({
      a: MarkdownLink,
      pre: (props: HTMLAttributes<HTMLPreElement> & { node?: HastElement }) => (
        <MarkdownPre {...props} contentLines={contentLines} />
      ),
    }),
    [contentLines]
  );

  return (
    <div className={className} data-testid={testId}>
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={components}
      >
        {content}
      </ReactMarkdown>
    </div>
  );
}
