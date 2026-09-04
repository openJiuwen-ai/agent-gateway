import { Loader2, SendHorizontal } from 'lucide-react';
import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type KeyboardEvent,
} from 'react';
import { useTranslation } from 'react-i18next';
import { useDesignerChatStore } from '../designerChatStore';
import { DesignerAssetsPanel } from './DesignerAssetsPanel';

type SidebarTab = 'assistant' | 'assets';

export function DesignerEmptyState({
  variant,
  errorMessage,
}: {
  variant: 'empty' | 'error';
  errorMessage?: string | null;
}) {
  const { t } = useTranslation();

  return (
    <div className="designer-page__state" data-testid="designer-empty-state" data-variant={variant}>
      <div className="designer-page__state-card">
        <h2 className="designer-page__state-title">
          {variant === 'error' ? t('designer.loadErrorTitle') : t('designer.emptyTitle')}
        </h2>
        <p className="designer-page__state-desc">
          {variant === 'error'
            ? errorMessage || t('designer.loadErrorFallback')
            : t('designer.emptyDescription')}
        </p>
      </div>
    </div>
  );
}

export function DesignerChatPanel() {
  const { t } = useTranslation();
  const messages = useDesignerChatStore((state) => state.messages);
  const bootstrapPhase = useDesignerChatStore((state) => state.bootstrapPhase);
  const appendMessage = useDesignerChatStore((state) => state.appendMessage);
  const bodyRef = useRef<HTMLDivElement>(null);
  const [draft, setDraft] = useState('');
  const [tab, setTab] = useState<SidebarTab>('assistant');

  const chatBusy = bootstrapPhase === 'thinking' || bootstrapPhase === 'bootstrapping';

  useEffect(() => {
    const el = bodyRef.current;
    if (!el || tab !== 'assistant') return;
    el.scrollTop = el.scrollHeight;
  }, [messages, tab]);

  const handleSend = useCallback(() => {
    const content = draft.trim();
    if (!content || chatBusy) return;
    setDraft('');
    appendMessage({
      role: 'user',
      content,
      kind: 'user',
    });
    appendMessage({
      role: 'assistant',
      content: t('designer.chat.notImplemented'),
      kind: 'not_implemented',
    });
  }, [appendMessage, chatBusy, draft, t]);

  const onKeyDown = useCallback(
    (event: KeyboardEvent<HTMLTextAreaElement>) => {
      if (event.key === 'Enter' && !event.shiftKey) {
        event.preventDefault();
        handleSend();
      }
    },
    [handleSend],
  );

  return (
    <aside
      className="designer-chat-panel designer-chat-panel--expanded"
      aria-label={t('designer.chat.title')}
      data-testid="designer-chat-panel"
      data-tab={tab}
    >
      <div className="designer-chat-panel__header" role="tablist" aria-label={t('designer.sidebar.tabsLabel')}>
        <button
          type="button"
          role="tab"
          aria-selected={tab === 'assistant'}
          className={`designer-chat-panel__tab${tab === 'assistant' ? ' is-active' : ''}`}
          data-testid="designer-sidebar-tab-assistant"
          onClick={() => setTab('assistant')}
        >
          {t('designer.sidebar.assistant')}
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={tab === 'assets'}
          className={`designer-chat-panel__tab${tab === 'assets' ? ' is-active' : ''}`}
          data-testid="designer-sidebar-tab-assets"
          onClick={() => setTab('assets')}
        >
          {t('designer.sidebar.assets')}
        </button>
      </div>

      {tab === 'assistant' ? (
        <>
          <div className="designer-chat-panel__body" ref={bodyRef} data-testid="designer-chat-panel-body">
            {messages.length === 0 ? (
              <p className="designer-chat-panel__empty">{t('designer.chat.emptyHint')}</p>
            ) : (
              <div className="designer-chat-panel__messages" data-testid="designer-chat-panel-messages">
                {messages.map((message) => (
                  <div
                    key={message.id}
                    className={`designer-chat-panel__message designer-chat-panel__message--${message.role}`}
                    data-testid="designer-chat-panel-message"
                    data-role={message.role}
                    data-kind={message.kind}
                  >
                    {message.kind === 'thinking' ? (
                      <span className="designer-chat-panel__thinking">
                        <Loader2 className="designer-chat-panel__thinking-icon" size={14} aria-hidden />
                        {message.content}
                      </span>
                    ) : (
                      message.content
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>
          <div className="designer-chat-panel__composer">
            <textarea
              className="designer-chat-panel__input"
              placeholder={t('designer.chat.inputPlaceholder')}
              value={draft}
              disabled={chatBusy}
              data-testid="designer-chat-panel-input"
              onChange={(event) => setDraft(event.target.value)}
              onKeyDown={onKeyDown}
            />
            <button
              type="button"
              className="designer-chat-panel__send"
              disabled={chatBusy || !draft.trim()}
              onClick={handleSend}
              aria-label={t('designer.chat.send')}
              data-testid="designer-chat-panel-send"
            >
              <SendHorizontal size={16} aria-hidden />
            </button>
          </div>
        </>
      ) : (
        <DesignerAssetsPanel />
      )}
    </aside>
  );
}
