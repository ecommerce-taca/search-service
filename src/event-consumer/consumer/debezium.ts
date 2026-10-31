/**
 * Debezium với JsonConverter có thể ghi giá trị scalar dưới dạng chuỗi JSON (`"abc"`)
 * hoặc struct có schema (`{"schema":...,"payload":"abc"}`); trả về chuỗi thuần.
 */
export function decodeDebeziumScalar(
  raw: Buffer | string | null | undefined,
): string | undefined {
  if (raw == null) return undefined;
  const text = (
    Buffer.isBuffer(raw) ? raw.toString('utf-8') : String(raw)
  ).trim();
  if (!text) return undefined;
  try {
    const parsed: unknown = JSON.parse(text);
    if (typeof parsed === 'string') return parsed || undefined;
    if (typeof parsed === 'number') return String(parsed);
    if (parsed && typeof parsed === 'object' && 'payload' in parsed) {
      const inner = (parsed as { payload: unknown }).payload;
      if (typeof inner === 'string' || typeof inner === 'number') {
        return String(inner) || undefined;
      }
    }
    return undefined;
  } catch {
    return text;
  }
}
