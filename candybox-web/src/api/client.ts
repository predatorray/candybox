import { z } from 'zod';

// The admin API origin: defaults to the same origin (so the SPA served from /ui/ talks back to
// /api/). In dev, vite's proxy rewrites /api/ → :9712 transparently. An explicit window override
// lets us point the SPA at a remote admin API during demos.
declare global {
  interface Window {
    CANDYBOX_API_BASE?: string;
  }
}

export function apiBase(): string {
  if (typeof window !== 'undefined' && window.CANDYBOX_API_BASE) {
    return window.CANDYBOX_API_BASE.replace(/\/+$/, '');
  }
  return '';
}

export async function apiGet<T>(path: string, schema: z.ZodType<T>): Promise<T> {
  const res = await fetch(`${apiBase()}${path}`, { headers: { Accept: 'application/json' } });
  if (!res.ok) {
    throw await readApiError(res);
  }
  const json = (await res.json()) as unknown;
  // Zod-parse here so a backend that drifts from the contract surfaces a clear schema error in the
  // network panel instead of silently failing later when a column tries to render `undefined`.
  return schema.parse(json);
}

/**
 * Mutating-request workhorse: handles JSON and binary bodies, surfaces structured admin-API errors,
 * and returns parsed JSON for 2xx responses (or `null` for 204 No Content). Splitting this from
 * {@link apiGet} keeps query callers simple while consolidating the create/upload/delete branches
 * so PUT and DELETE round-trips stay one obvious call site each.
 */
export async function apiSend(
  method: 'POST' | 'PUT' | 'DELETE',
  path: string,
  init?: { jsonBody?: unknown; bodyBytes?: BodyInit; contentType?: string },
): Promise<unknown> {
  const headers: Record<string, string> = { Accept: 'application/json' };
  let body: BodyInit | undefined;
  if (init?.jsonBody !== undefined) {
    headers['Content-Type'] = 'application/json';
    body = JSON.stringify(init.jsonBody);
  } else if (init?.bodyBytes !== undefined) {
    body = init.bodyBytes;
    // Leave undefined → the browser will inherit the File/Blob's contentType, which is usually
    // what we want when uploading a File picker selection.
    if (init.contentType) {
      headers['Content-Type'] = init.contentType;
    }
  }
  const res = await fetch(`${apiBase()}${path}`, { method, headers, body });
  if (!res.ok) {
    throw await readApiError(res);
  }
  if (res.status === 204) {
    return null;
  }
  const text = await res.text();
  return text ? (JSON.parse(text) as unknown) : null;
}

/**
 * Maps a non-OK response into an {@link ApiError}, decoding the admin API's
 * {@code {"error": "...", "message": "..."}} shape so the UI can show a human-friendly reason
 * instead of a raw JSON blob in the snackbar.
 */
async function readApiError(res: Response): Promise<ApiError> {
  const body = await res.text().catch(() => '');
  try {
    const parsed = JSON.parse(body) as { error?: string; message?: string };
    if (parsed && (parsed.error || parsed.message)) {
      return new ApiError(res.status, body, parsed.error ?? '', parsed.message ?? '');
    }
  } catch {
    // Not JSON; fall through and keep the raw body as the message.
  }
  return new ApiError(res.status, body);
}

export class ApiError extends Error {
  constructor(
    public status: number,
    public body: string,
    public code: string = '',
    public detail: string = '',
  ) {
    super(detail || code || `${status} ${body || 'request failed'}`);
    this.name = 'ApiError';
  }
}

// ---- Cluster ---------------------------------------------------------------

// Backend returns JSON `null` (not omitted keys) for fields it doesn't know yet — e.g. a box whose
// owner hasn't reported runtime stats. Zod's `.optional()` only accepts `undefined`, so use
// `.nullish()` (= nullable + optional) for every field the admin API may send as null. Mixing
// `.optional()` with a null payload was the source of the "Boxes tab keeps showing zod errors" bug.
export const NodeRowSchema = z.object({
  nodeId: z.string(),
  address: z.string().nullish(),
  ready: z.boolean().nullish(),
  ownedBoxCount: z.number().int().nonnegative().nullish(),
});
export type NodeRow = z.infer<typeof NodeRowSchema>;

export const ClusterSchema = z.object({
  nodes: z.array(NodeRowSchema),
  boxCount: z.number().int().nonnegative(),
  ownerless: z.array(z.string()),
  stub: z.boolean().optional(),
});
export type Cluster = z.infer<typeof ClusterSchema>;

export const fetchCluster = () => apiGet('/api/cluster', ClusterSchema);

// ---- Boxes -----------------------------------------------------------------

export const BoxRowSchema = z.object({
  name: z.string(),
  owner: z.string().nullish(),
  candyCount: z.number().int().nonnegative().nullish(),
  sizeBytes: z.number().int().nonnegative().nullish(),
  manifestVersion: z.number().int().nonnegative().nullish(),
  fencingToken: z.number().int().nonnegative().nullish(),
  hlc: z.string().nullish(),
});
export type BoxRow = z.infer<typeof BoxRowSchema>;

export const BoxesSchema = z.object({
  boxes: z.array(BoxRowSchema),
});
export type Boxes = z.infer<typeof BoxesSchema>;

export const fetchBoxes = () => apiGet('/api/boxes', BoxesSchema);

export const BoxDetailSchema = z.object({
  name: z.string(),
  owner: z.string().nullish(),
  manifestVersion: z.number().int().nonnegative().nullish(),
  fencingToken: z.number().int().nonnegative().nullish(),
  hlc: z.string().nullish(),
  candyCount: z.number().int().nonnegative().nullish(),
  sizeBytes: z.number().int().nonnegative().nullish(),
});
export type BoxDetail = z.infer<typeof BoxDetailSchema>;

export const fetchBox = (name: string) =>
  apiGet(`/api/boxes/${encodeURIComponent(name)}`, BoxDetailSchema);

export const CandyRowSchema = z.object({
  key: z.string(),
  contentLength: z.number().int().nonnegative(),
  createdAtMillis: z.number().int().nonnegative(),
});
export type CandyRow = z.infer<typeof CandyRowSchema>;

export const CandyListingSchema = z.object({
  entries: z.array(CandyRowSchema),
  nextStartAfter: z.string().nullish(),
});
export type CandyListing = z.infer<typeof CandyListingSchema>;

export const fetchCandies = (name: string, prefix?: string, startAfter?: string) => {
  const qs = new URLSearchParams();
  if (prefix) qs.set('prefix', prefix);
  if (startAfter) qs.set('startAfter', startAfter);
  const suffix = qs.toString() ? `?${qs}` : '';
  return apiGet(`/api/boxes/${encodeURIComponent(name)}/objects${suffix}`, CandyListingSchema);
};

// ---- LSM internals ---------------------------------------------------------

export const LsmBoxSchema = z.object({
  box: z.string(),
  owner: z.string().nullish(),
  manifestVersion: z.number().int().nonnegative().nullish(),
  sstableLedgerCount: z.number().int().nonnegative().nullish(),
  syrupLedgerCount: z.number().int().nonnegative().nullish(),
  walLedgerCount: z.number().int().nonnegative().nullish(),
  inFlightCompactions: z.number().int().nonnegative().nullish(),
  gcBacklog: z.number().int().nonnegative().nullish(),
});
export type LsmBox = z.infer<typeof LsmBoxSchema>;

export const LsmSchema = z.object({
  boxes: z.array(LsmBoxSchema),
});
export type Lsm = z.infer<typeof LsmSchema>;

export const fetchLsm = () => apiGet('/api/lsm', LsmSchema);

// ---- Metrics ---------------------------------------------------------------

export const MetricSampleSchema = z.object({
  t: z.number(),
  v: z.number(),
});
export type MetricSample = z.infer<typeof MetricSampleSchema>;

export const MetricSeriesSchema = z.object({
  name: z.string(),
  labels: z.record(z.string()).optional(),
  samples: z.array(MetricSampleSchema),
});
export type MetricSeries = z.infer<typeof MetricSeriesSchema>;

export const TimeseriesSchema = z.object({
  series: z.array(MetricSeriesSchema),
  windowSeconds: z.number().int().positive(),
});
export type Timeseries = z.infer<typeof TimeseriesSchema>;

export const fetchTimeseries = (names: string[]) =>
  apiGet(
    `/api/metrics/timeseries?names=${encodeURIComponent(names.join(','))}`,
    TimeseriesSchema,
  );

// ---- Mutating ops ----------------------------------------------------------
//
// These wrap the admin API's POST/PUT/DELETE endpoints. The dashboard runs without auth — same
// posture as the S3 gateway — so any operator with network access to the admin port can call them.
// Tighten this before exposing the dashboard outside a trusted network.

export const createBox = (name: string) =>
  apiSend('POST', '/api/boxes', { jsonBody: { name } });

export const deleteBox = (name: string, force = false) =>
  apiSend(
    'DELETE',
    `/api/boxes/${encodeURIComponent(name)}${force ? '?force=true' : ''}`,
  );

/**
 * Uploads one Candy. {@code file} is the {@link File} from the {@code <input type="file" />} —
 * its bytes stream straight through fetch, and its {@code type} becomes the Content-Type the
 * server records. The {@code key} may contain slashes; we split on '/' so each segment is encoded
 * separately, preserving the S3-style "folder/file.txt" hierarchy in the URL.
 */
export const uploadCandy = (box: string, key: string, file: File) =>
  apiSend(
    'PUT',
    `/api/boxes/${encodeURIComponent(box)}/objects/${encodeCandyKey(key)}`,
    { bodyBytes: file, contentType: file.type || 'application/octet-stream' },
  );

export const deleteCandy = (box: string, key: string) =>
  apiSend(
    'DELETE',
    `/api/boxes/${encodeURIComponent(box)}/objects/${encodeCandyKey(key)}`,
  );

function encodeCandyKey(key: string): string {
  // Encode each path segment so reserved characters survive a round-trip, but keep '/' as a
  // delimiter so the admin API can route "folder/foo.txt" without us double-encoding the slash.
  return key.split('/').map(encodeURIComponent).join('/');
}
