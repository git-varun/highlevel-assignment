// One error type for "expected" failures, one handler that turns everything into JSON.

export class HttpError extends Error {
  constructor(status, code, message, details, retryAfterSec) {
    super(message);
    this.status = status;
    this.code = code;        // stable, machine-readable (clients branch on this, not on message)
    this.details = details;
    this.retryAfterSec = retryAfterSec;
  }
}

export const tooManyRequests = (code, msg, details, retryAfterSec) =>
  new HttpError(429, code, msg, details, retryAfterSec);

export const badRequest = (msg, details) => new HttpError(400, 'BAD_REQUEST', msg, details);
export const notFound = (msg) => new HttpError(404, 'NOT_FOUND', msg);
export const conflict = (msg, details) => new HttpError(409, 'CONFLICT', msg, details);
export const unprocessable = (code, msg, details) => new HttpError(422, code, msg, details);

// Postgres error codes we expect and can explain to a client.
// Anything else is a bug → 500, with details only in the server log.
const PG_MAP = {
  '23503': () => unprocessable('INVALID_REFERENCE', 'Referenced record does not exist in this workspace'),
  '22P02': () => badRequest('Malformed value'),
  '22003': () => badRequest('Numeric value out of range'),
  '23514': () => unprocessable('CONSTRAINT_VIOLATION', 'Value violates a data constraint'),
  '55P03': () => new HttpError(503, 'BUSY', 'Record is busy (being updated by a bulk job); retry shortly'),
  '57014': () => new HttpError(503, 'TIMEOUT', 'Query timed out; retry shortly'),
};

// Express 5 forwards rejected promises from async handlers here automatically.
// eslint-disable-next-line no-unused-vars
export function errorHandler(err, req, res, _next) {
  const mapped = err instanceof HttpError ? err : PG_MAP[err.code]?.();
  if (mapped) {
    if (mapped.retryAfterSec) res.set('Retry-After', String(mapped.retryAfterSec));
    else if (mapped.status === 503) res.set('Retry-After', '1');
    return res.status(mapped.status).json({ error: { code: mapped.code, message: mapped.message, details: mapped.details } });
  }
  // body-parser errors (bad JSON 400, body too large 413, bad charset/encoding 415) carry
  // their own 4xx status and expose=true. They are client errors — never a 500.
  if (err.expose && err.status >= 400 && err.status < 500) {
    const code = err.type === 'entity.too.large' ? 'PAYLOAD_TOO_LARGE' : 'BAD_REQUEST';
    const message = err.type === 'entity.parse.failed' ? 'Invalid JSON body' : err.message;
    return res.status(err.status).json({ error: { code, message } });
  }
  console.error(`[api] ${req.method} ${req.originalUrl}`, err);
  res.status(500).json({ error: { code: 'INTERNAL', message: 'Internal error' } });
}
