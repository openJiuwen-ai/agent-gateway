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
import { getSvgNaturalHeight, getSvgNaturalWidth } from '../../utils/svgDimensions';
import {
  Copy,
  Check,
  ZoomIn,
  ZoomOut,
  RotateCcw,
} from 'lucide-react';
import './MarkdownRenderer.css';

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
  flowchart: { useMaxWidth: false },
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
      className="markdown-toolbar-btn"
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
        'markdown-toggle-pill',
        active && 'markdown-toggle-pill--active'
      )}
    >
      {children}
    </button>
  );
}

function clampScale(s: number): number {
  return Math.min(Math.max(s, 0.25), 3);
}

const MERMAID_CANVAS_MAX_HEIGHT = 600;
const MERMAID_CANVAS_TOP_OFFSET = 24;
const MERMAID_CANVAS_BOTTOM_OFFSET = 24;
const MERMAID_MIN_FIT_SCALE = 0.5;

function MermaidBlock({ code }: { code: string }) {
  const { t } = useTranslation();
  const diagramId = `mermaid-${useId().replace(/[^A-Za-z0-9_-]/g, '_')}`;
  const [renderState, setRenderState] = useState<MermaidRenderState>({
    status: 'loading',
    svg: '',
  });
  const [viewMode, setViewMode] = useState<'image' | 'code'>('image');
  const [scale, setScale] = useState(1);
  const [fitScale, setFitScale] = useState(1);
  const [copied, setCopied] = useState(false);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  const [isDragging, setIsDragging] = useState(false);
  const [canvasHeight, setCanvasHeight] = useState(MERMAID_CANVAS_MAX_HEIGHT);
  const [alignTop, setAlignTop] = useState(false);
  const isDraggingRef = useRef(false);
  const dragStartRef = useRef({ x: 0, y: 0 });
  const panStartRef = useRef({ x: 0, y: 0 });
  const canvasRef = useRef<HTMLDivElement>(null);

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

  useEffect(() => {
    if (renderState.status !== 'rendered' || viewMode !== 'image') return;
    const svg = canvasRef.current?.querySelector('svg');
    if (!svg) return;

    const updateDimensions = () => {
      const naturalHeight = getSvgNaturalHeight(svg);
      const naturalWidth = getSvgNaturalWidth(svg);
      if (naturalHeight <= 0) return;

      const containerWidth = canvasRef.current?.clientWidth ?? 0;
      const availableHeight =
        MERMAID_CANVAS_MAX_HEIGHT -
        MERMAID_CANVAS_TOP_OFFSET -
        MERMAID_CANVAS_BOTTOM_OFFSET;

      // Fit wide diagrams to the container width so they are not squeezed or
      // clipped. For tall diagrams, also scale down so users can see more at
      // once, but cap the shrink to keep text readable.
      const scaleToFitWidth =
        containerWidth > 0 && naturalWidth > 0 ? containerWidth / naturalWidth : 1;
      const scaleToFitHeight =
        naturalHeight > 0 ? availableHeight / naturalHeight : 1;
      const nextFitScale = clampScale(
        Math.min(scaleToFitWidth, Math.max(MERMAID_MIN_FIT_SCALE, scaleToFitHeight)),
      );

      const scaledHeight = naturalHeight * nextFitScale;
      const contentHeight =
        scaledHeight + MERMAID_CANVAS_TOP_OFFSET + MERMAID_CANVAS_BOTTOM_OFFSET;
      const nextCanvasHeight = Math.min(MERMAID_CANVAS_MAX_HEIGHT, contentHeight);

      setFitScale(nextFitScale);
      setScale(nextFitScale);
      setPan({ x: 0, y: 0 });
      setCanvasHeight(nextCanvasHeight);
      // Center small diagrams, but align tall diagrams to the top so users see
      // the beginning and can pan down.
      setAlignTop(contentHeight > MERMAID_CANVAS_MAX_HEIGHT);
    };

    updateDimensions();
    const canvas = canvasRef.current;
    if (!canvas) return;

    const observer = new ResizeObserver(updateDimensions);
    observer.observe(canvas);
    return () => observer.disconnect();
  }, [renderState.status, renderState.svg, viewMode]);

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

  const panTransform = `translate(${pan.x}px, ${pan.y}px)`;
  const wrapperStyle = alignTop
    ? {
        top: MERMAID_CANVAS_TOP_OFFSET,
        transformOrigin: 'top center' as const,
        transform: `translate(-50%, 0) ${panTransform} scale(${scale})`,
      }
    : {
        top: '50%' as const,
        transformOrigin: 'center center' as const,
        transform: `translate(-50%, -50%) ${panTransform} scale(${scale})`,
      };

  return (
    <div
      className="mermaid-diagram"
      data-mermaid-status="rendered"
    >
      {/* Toolbar */}
      <div className="mermaid-diagram__toolbar">
        {/* Left: View toggle */}
        <div className="mermaid-diagram__view-toggle">
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
        <div className="mermaid-diagram__actions">
          <ToolbarButton title={t('mermaid.copyCode')} onClick={handleCopy}>
            {copied ? (
              <Check size={15} className="text-ok" />
            ) : (
              <Copy size={15} />
            )}
          </ToolbarButton>
          {viewMode === 'image' && (
            <>
              <div className="mermaid-diagram__toolbar-divider" />
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
                onClick={() => { setScale(fitScale); setPan({ x: 0, y: 0 }); }}
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
          ref={canvasRef}
          className={clsx('mermaid-canvas', isDragging && 'mermaid-canvas--dragging')}
          style={{ height: canvasHeight }}
          onMouseDown={(e) => { e.preventDefault(); startDrag(e.clientX, e.clientY); }}
          onMouseMove={(e) => moveDrag(e.clientX, e.clientY)}
          onMouseUp={endDrag}
          onMouseLeave={endDrag}
          onTouchStart={(e) => { const touch = e.touches[0]; startDrag(touch.clientX, touch.clientY); }}
          onTouchMove={(e) => { const touch = e.touches[0]; moveDrag(touch.clientX, touch.clientY); }}
          onTouchEnd={endDrag}
        >
          <div
            className="mermaid-svg-wrapper"
            style={wrapperStyle}
            dangerouslySetInnerHTML={{ __html: renderState.svg }}
          />
        </div>
      ) : (
        <div className="mermaid-code-view">
          <pre>
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
