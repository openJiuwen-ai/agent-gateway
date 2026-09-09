/** Pick which Designer graph to open after a project list/get. */

export function resolveDesignerGraphToLoad(input: {
  currentId?: string | null;
  isPreview?: boolean;
  listedIds: string[];
}): string | null {
  const currentId = String(input.currentId ?? '').trim();
  if (currentId && !input.isPreview) {
    return currentId;
  }
  return input.listedIds.find((id) => String(id || '').trim()) ?? null;
}
