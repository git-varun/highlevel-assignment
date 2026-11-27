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
  if (err.type === 'entity.parse.failed') {
    return res.status(400).json({ error: { code: 'BAD_REQUEST', message: 'Invalid JSON body' } });
  }
  console.error(`[api] ${req.method} ${req.originalUrl}`, err);
  res.status(500).json({ error: { code: 'INTERNAL', message: 'Internal error' } });
}
