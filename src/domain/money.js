// Money as STRINGS end-to-end. Never Number(): floats can't represent most decimals
// exactly, and above 2^53 they silently change digits ("9007199254740993" → ...992).
// Column is numeric(14,2): at most 12 integer digits + 2 decimals.
const MONEY_RE = /^(\d{1,12})(?:\.(\d{1,2}))?$/;

/** Validate + canonicalise: "0100" → "100.00", "5.5" → "5.50". Returns null if invalid. */
export function canonicalMoney(v) {
  const m = MONEY_RE.exec(typeof v === 'number' ? String(v) : v);
  if (!m) return null;
  const int = m[1].replace(/^0+(?=\d)/, '');
  return `${int}.${(m[2] ?? '').padEnd(2, '0')}`;
}

/** Exact comparison via integer cents (BigInt), no float rounding. */
export const moneyCents = (canonical) => BigInt(canonical.replace('.', ''));
