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

/** Manually attached materials (uploads). Linked media nodes are derived from edges. */
export type MediaMaterialSlot = {
  id: string;
  filename: string;
  mime_type?: string;
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

function normalizeMaterials(raw: unknown): MediaMaterialSlot[] {
  if (!Array.isArray(raw)) return [];
  const out: MediaMaterialSlot[] = [];
  for (const item of raw) {
    if (!item || typeof item !== 'object') continue;
    const record = item as Record<string, unknown>;
    const id = typeof record.id === 'string' ? record.id.trim() : '';
    const filename =
      typeof record.filename === 'string'
        ? record.filename.trim()
        : typeof record.label === 'string'
          ? record.label.trim()
          : '';
    if (!id || !filename) continue;
    out.push({
      id,
      filename,
      ...(typeof record.mime_type === 'string' && record.mime_type
        ? { mime_type: record.mime_type }
        : {}),
    });
  }
  return out;
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
    materials: normalizeMaterials(raw.materials),
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

export function writeMediaMaterials(
  config: Record<string, unknown> | undefined,
  materials: MediaMaterialSlot[],
): Record<string, unknown> {
  return {
    ...(config ?? {}),
    materials,
  };
}
