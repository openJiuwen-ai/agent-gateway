/** Node config helpers for Designer toolbar (generate / upload / edit). */

export type MediaInteractionMode = 'generate' | 'upload' | 'edit';

export type MediaGenerateConfig = {
  prompt?: string;
  aspect_ratio?: string;
  resolution?: string;
  duration?: string;
  has_audio?: boolean;
  count?: number;
};

export type MediaUploadConfig = {
  filename?: string;
};

export type MediaEditConfig = {
  content?: string;
};

export type MediaMaterialSlot = {
  id: string;
  label?: string;
};

export type MediaNodeConfig = {
  inputs?: string[];
  interaction_mode?: MediaInteractionMode;
  generate?: MediaGenerateConfig;
  upload?: MediaUploadConfig;
  edit?: MediaEditConfig;
  materials?: MediaMaterialSlot[];
};

export function isMediaNodeType(nodeType: string): boolean {
  return nodeType === 'image' || nodeType === 'video' || nodeType === 'audio';
}

export function supportsNodeToolbar(nodeType: string): boolean {
  return isMediaNodeType(nodeType) || nodeType === 'text';
}

function normalizeInteractionMode(
  raw: string | undefined,
  nodeType?: string,
): MediaInteractionMode {
  if (raw === 'generate') return 'generate';
  if (nodeType === 'text') {
    return raw === 'edit' || raw === 'upload' ? 'edit' : 'generate';
  }
  return raw === 'upload' ? 'upload' : 'generate';
}

export function readMediaConfig(
  config: Record<string, unknown> | undefined,
  nodeType?: string,
): MediaNodeConfig {
  const raw = (config ?? {}) as MediaNodeConfig;
  const mode = normalizeInteractionMode(
    typeof raw.interaction_mode === 'string' ? raw.interaction_mode : undefined,
    nodeType,
  );
  return {
    ...raw,
    interaction_mode: mode,
    generate: {
      prompt: raw.generate?.prompt ?? '',
      aspect_ratio: raw.generate?.aspect_ratio ?? '16:9',
      resolution: raw.generate?.resolution ?? '1080p',
      duration: raw.generate?.duration ?? '5s',
      has_audio: raw.generate?.has_audio ?? true,
      count: raw.generate?.count ?? 1,
    },
    upload: {
      filename: raw.upload?.filename ?? '',
    },
    edit: {
      content: raw.edit?.content ?? '',
    },
    materials: Array.isArray(raw.materials) && raw.materials.length > 0
      ? raw.materials
      : [
          { id: 'mat_1', label: '素材' },
          { id: 'mat_2', label: '素材' },
          { id: 'mat_3', label: '素材' },
        ],
  };
}

export function writeMediaInteractionMode(
  config: Record<string, unknown> | undefined,
  mode: MediaInteractionMode,
): Record<string, unknown> {
  return {
    ...(config ?? {}),
    interaction_mode: mode,
  };
}

export function writeMediaGeneratePatch(
  config: Record<string, unknown> | undefined,
  patch: Partial<MediaGenerateConfig>,
): Record<string, unknown> {
  const current = readMediaConfig(config);
  return {
    ...(config ?? {}),
    interaction_mode:
      current.interaction_mode === 'upload' || current.interaction_mode === 'edit'
        ? current.interaction_mode
        : 'generate',
    generate: {
      ...current.generate,
      ...patch,
    },
  };
}

export function writeMediaUploadPatch(
  config: Record<string, unknown> | undefined,
  patch: Partial<MediaUploadConfig>,
): Record<string, unknown> {
  const current = readMediaConfig(config);
  return {
    ...(config ?? {}),
    interaction_mode: 'upload',
    upload: {
      ...current.upload,
      ...patch,
    },
  };
}

export function writeMediaEditPatch(
  config: Record<string, unknown> | undefined,
  patch: Partial<MediaEditConfig>,
): Record<string, unknown> {
  const current = readMediaConfig(config, 'text');
  return {
    ...(config ?? {}),
    interaction_mode: 'edit',
    edit: {
      ...current.edit,
      ...patch,
    },
  };
}
