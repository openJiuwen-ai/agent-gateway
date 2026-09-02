import { ChevronLeft, ChevronRight } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useDesignerStore } from '../designerStore';

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
  const chatCollapsed = useDesignerStore((state) => state.chatCollapsed);
  const setChatCollapsed = useDesignerStore((state) => state.setChatCollapsed);

  return (
    <aside
      className={`designer-chat-panel${chatCollapsed ? ' designer-chat-panel--collapsed' : ' designer-chat-panel--expanded'}`}
      aria-label={t('designer.chat.title')}
      data-testid="designer-chat-panel"
      data-collapsed={chatCollapsed ? 'true' : 'false'}
    >
      <div className="designer-chat-panel__header">
        {!chatCollapsed ? <span className="designer-chat-panel__title">{t('designer.chat.title')}</span> : null}
        <button
          type="button"
          className="designer-chat-panel__toggle"
          onClick={() => setChatCollapsed(!chatCollapsed)}
          aria-label={chatCollapsed ? t('designer.chat.expand') : t('designer.chat.collapse')}
          data-testid="designer-chat-panel-toggle"
        >
          {chatCollapsed ? <ChevronRight size={16} aria-hidden /> : <ChevronLeft size={16} aria-hidden />}
        </button>
      </div>
      {!chatCollapsed ? (
        <>
          <div className="designer-chat-panel__body" data-testid="designer-chat-panel-body">
            <p className="designer-chat-panel__empty">{t('designer.chat.emptyHint')}</p>
          </div>
          <div className="designer-chat-panel__composer">
            <textarea
              className="designer-chat-panel__input"
              placeholder={t('designer.chat.inputPlaceholder')}
              readOnly
              aria-readonly="true"
              data-testid="designer-chat-panel-input"
            />
          </div>
        </>
      ) : null}
    </aside>
  );
}
