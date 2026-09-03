import { Loader2 } from 'lucide-react';
import { useEffect, useRef } from 'react';
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
  const bootstrapInProgress = useDesignerStore((state) => state.bootstrapInProgress);
  const loadForProject = useDesignerStore((state) => state.loadForProject);
  // Tasks→Design bootstrap 结束后 bootstrapInProgress 会变 false，若立刻 list/get
  //（尤其 projectId 为空或与新建 project 不一致），会把刚 apply 的图刷成 empty。
  const skipLoadAfterBootstrapRef = useRef(false);

  useEffect(() => {
    if (bootstrapInProgress) {
      skipLoadAfterBootstrapRef.current = true;
      return;
    }
    if (skipLoadAfterBootstrapRef.current) {
      skipLoadAfterBootstrapRef.current = false;
      return;
    }
    void loadForProject(projectId);
  }, [bootstrapInProgress, loadForProject, projectId]);

  const showCanvas = loadStatus === 'ready' && domainGraph;
  const showEmpty = loadStatus === 'empty';
  const showError = loadStatus === 'error';
  const showLoading =
    loadStatus === 'loading' || loadStatus === 'idle' || loadStatus === 'bootstrapping';

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
              <p className="designer-page__state-desc">
                {loadStatus === 'bootstrapping' ? t('designer.chat.thinking') : t('designer.loading')}
              </p>
            </div>
          </div>
        ) : null}

        {showEmpty ? <DesignerEmptyState variant="empty" /> : null}
        {showError ? <DesignerEmptyState variant="error" errorMessage={loadError} /> : null}
      </div>
    </div>
  );
}
