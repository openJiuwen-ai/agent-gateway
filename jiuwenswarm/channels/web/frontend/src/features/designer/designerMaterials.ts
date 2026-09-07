import {
  designerAssetPreviewUrl,
  designerAssetTextUrl,
  isPlaceholderAsset,
} from './designerAssetUrl';
import type {
  AssetRef,
  DesignerExecutionGraph,
  DesignerExecutionRun,
  DesignerGraphNode,
  DesignerNodeState,
} from './executionGraphTypes';

export type DesignerPreviewKind = 'video' | 'image' | 'audio' | 'text' | 'file' | 'placeholder';

export const DESIGNER_EDITABLE_ROLES = new Set(['brief', 'storyboard']);
export const DESIGNER_MATERIAL_SAVED_EVENT = 'designer-material-saved';

export type DesignerMaterial = {
  id: string;
  nodeId: string;
  label: string;
  kind: string;
  role?: string;
  uri: string;
  mimeType?: string;
  placeholder: boolean;
  previewUrl: string | null;
  textUrl: string | null;
  editable?: boolean;
};

export function isEditableDesignerMaterial(material: DesignerMaterial): boolean {
  if (material.placeholder || !material.textUrl) return false;
  if (material.editable) return true;
  if (DESIGNER_EDITABLE_ROLES.has(material.role || '')) return true;
  return material.kind === 'text' || material.kind === 'table';
}

export function classifyDesignerOutput(input: {
  kind?: string;
  mimeType?: string;
  label?: string;
  placeholder?: boolean;
  previewUrl?: string | null;
  textUrl?: string | null;
}): DesignerPreviewKind {
  if (input.placeholder || (!input.previewUrl && !input.textUrl)) return 'placeholder';
  if (input.kind === 'video' || (input.mimeType || '').startsWith('video/')) return 'video';
  if (input.kind === 'image' || (input.mimeType || '').startsWith('image/')) return 'image';
  if (input.kind === 'audio' || (input.mimeType || '').startsWith('audio/')) return 'audio';
  if (
    input.kind === 'text' ||
    input.kind === 'table' ||
    (input.mimeType || '').startsWith('text/') ||
    (input.label || '').endsWith('.md')
  ) {
    return 'text';
  }
  return 'file';
}

function refsFromState(
  state: DesignerNodeState | undefined,
  kind: 'accepted' | 'candidate',
): AssetRef[] {
  if (!state) return [];
  if (kind === 'candidate') {
    if (state.candidate_output_refs && state.candidate_output_refs.length > 0) {
      return state.candidate_output_refs;
    }
    return state.candidate_output_ref ? [state.candidate_output_ref] : [];
  }
  if (state.output_refs && state.output_refs.length > 0) return state.output_refs;
  return state.output_ref ? [state.output_ref] : [];
}

export function materialsFromRefs(
  node: Pick<DesignerGraphNode, 'id' | 'label' | 'type' | 'config'>,
  refs: AssetRef[],
  idPrefix: string,
): DesignerMaterial[] {
  const role = typeof node.config?.role === 'string' ? node.config.role : '';
  return refs.flatMap((ref, index) => {
    if (!ref?.uri) return [];
    const textUrl = designerAssetTextUrl(ref.uri);
    const placeholder = isPlaceholderAsset(ref.uri);
    const kind = ref.kind || node.type;
    const editable =
      !placeholder &&
      Boolean(textUrl) &&
      (DESIGNER_EDITABLE_ROLES.has(role) || kind === 'text' || kind === 'table');
    return [
      {
        id: `${idPrefix}:${index}`,
        nodeId: node.id,
        label: ref.label || (refs.length > 1 ? `${node.label} ${index + 1}` : node.label),
        kind,
        role,
        uri: ref.uri,
        mimeType: ref.mime_type,
        placeholder,
        previewUrl: designerAssetPreviewUrl(ref.uri),
        textUrl,
        editable,
      },
    ];
  });
}

export function hasPendingDesignerRevision(state: DesignerNodeState | undefined): boolean {
  const uri = state?.candidate_output_ref?.uri || state?.candidate_output_refs?.[0]?.uri;
  return Boolean(uri) && !isPlaceholderAsset(uri);
}

export type DesignerPendingRevision = {
  nodeId: string;
  label: string;
  original: DesignerMaterial[];
  incoming: DesignerMaterial[];
};

export function collectPendingRevisions(
  graph: DesignerExecutionGraph | null | undefined,
  run: DesignerExecutionRun | null | undefined,
): DesignerPendingRevision[] {
  if (!graph || !run) return [];
  return graph.nodes.flatMap((node) => {
    const state = run.node_states?.[node.id];
    if (!hasPendingDesignerRevision(state)) return [];
    const original = materialsFromRefs(node, refsFromState(state, 'accepted'), `${node.id}:orig`);
    const incoming = materialsFromRefs(node, refsFromState(state, 'candidate'), `${node.id}:new`);
    if (original.length === 0 || incoming.length === 0) return [];
    return [{ nodeId: node.id, label: node.label, original, incoming }];
  });
}

export function collectDesignerMaterials(
  graph: DesignerExecutionGraph | null | undefined,
  run: DesignerExecutionRun | null | undefined,
): DesignerMaterial[] {
  if (!graph) return [];
  return graph.nodes.flatMap((node) => {
    const fromRun = refsFromState(run?.node_states?.[node.id], 'accepted');
    const refs =
      fromRun.length > 0
        ? fromRun
        : node.output_ref?.uri
          ? [node.output_ref]
          : [];
    return materialsFromRefs(node, refs, node.id);
  });
}

const PREFERRED_KINDS = ['video', 'image', 'audio', 'table', 'text'] as const;

export function preferredDesignerMaterial(
  materials: DesignerMaterial[],
): DesignerMaterial | undefined {
  const usable = (item: DesignerMaterial) =>
    !item.placeholder && Boolean(item.previewUrl || item.textUrl);
  for (const kind of PREFERRED_KINDS) {
    const found = materials.find((item) => item.kind === kind && usable(item));
    if (found) return found;
  }
  return materials.find(usable) ?? materials[0];
}
