/** Media node config helpers for Designer toolbar (generate / upload). */

export type MediaInteractionMode = 'generate' | 'upload';

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

export type MediaMaterialSlot = {
  id: string;
  label?: string;
};

export type MediaNodeConfig = {
  inputs?: string[];
  interaction_mode?: MediaInteractionMode;
  generate?: MediaGenerateConfig;
  upload?: MediaUploadConfig;
  materials?: MediaMaterialSlot[];
};

export function isMediaNodeType(nodeType: string): boolean {
  return nodeType === 'image' || nodeType === 'video' || nodeType === 'audio';
}

export function readMediaConfig(config: Record<string, unknown> | undefined): MediaNodeConfig {
  const raw = (config ?? {}) as MediaNodeConfig;
  const mode = raw.interaction_mode === 'upload' ? 'upload' : 'generate';
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
    interaction_mode: current.interaction_mode ?? 'generate',
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
    interaction_mode: current.interaction_mode ?? 'upload',
    upload: {
      ...current.upload,
      ...patch,
    },
  };
}
