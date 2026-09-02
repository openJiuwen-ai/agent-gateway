import { Loader2 } from 'lucide-react';
import { useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import { useDesignerStore } from '../designerStore';
import { DesignerCanvas } from './DesignerCanvas';
import { DesignerChatPanel, DesignerEmptyState } from './DesignerChatPanel';
import './DesignerPage.css';

type DesignerPageProps = {
  projectId?: string;
};

export function DesignerPage({ projectId }: DesignerPageProps) {
  const { t } = useTranslation();
  const loadStatus = useDesignerStore((state) => state.loadStatus);
  const loadError = useDesignerStore((state) => state.loadError);
  const domainGraph = useDesignerStore((state) => state.domainGraph);
  const loadForProject = useDesignerStore((state) => state.loadForProject);

  useEffect(() => {
    void loadForProject(projectId);
  }, [loadForProject, projectId]);

  const showCanvas = loadStatus === 'ready' && domainGraph;
  const showEmpty = loadStatus === 'empty';
  const showError = loadStatus === 'error';
  const showLoading = loadStatus === 'loading' || loadStatus === 'idle';

  return (
    <div className="designer-page app-section" data-testid="designer-page">
      <div className="designer-page__workspace">
        <DesignerChatPanel />

        {showCanvas ? (
          <>
            <div className="designer-page__header" data-testid="designer-page-title">
              {domainGraph.title}
            </div>
            <DesignerCanvas graph={domainGraph} />
          </>
        ) : null}

        {showLoading ? (
          <div className="designer-page__state" data-testid="designer-loading-state">
            <div className="designer-page__state-card">
              <Loader2 className="mx-auto mb-3 animate-spin" size={24} aria-hidden />
              <p className="designer-page__state-desc">{t('designer.loading')}</p>
            </div>
          </div>
        ) : null}

        {showEmpty ? <DesignerEmptyState variant="empty" /> : null}
        {showError ? <DesignerEmptyState variant="error" errorMessage={loadError} /> : null}
      </div>
    </div>
  );
}
