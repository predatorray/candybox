import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  ApiError,
  BoxesSchema,
  BoxDetailSchema,
  ClusterSchema,
  CandyListingSchema,
  LsmSchema,
  createBox,
  deleteBox,
  deleteCandy,
  uploadCandy,
} from './client';

// Regression: the admin API emits JSON `null` (not omitted keys) for runtime stats that the
// owner hasn't reported yet. The schemas used `.optional()`, which only accepts `undefined`, so
// the Boxes tab kept rendering a zod error blob instead of "—". These tests pin the contract:
// every backend-nullable field must parse `null` cleanly.

beforeEach(() => {
  vi.unstubAllGlobals();
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe('api schemas accept null for unreported fields', () => {
  it('BoxesSchema accepts the real null-bearing payload', () => {
    const payload = {
      boxes: [
        {
          name: 'photos',
          owner: '1',
          candyCount: null,
          sizeBytes: null,
          manifestVersion: null,
          fencingToken: null,
          hlc: null,
        },
      ],
    };
    expect(() => BoxesSchema.parse(payload)).not.toThrow();
  });

  it('BoxDetailSchema accepts all-null runtime fields', () => {
    expect(() =>
      BoxDetailSchema.parse({
        name: 'photos',
        owner: null,
        manifestVersion: null,
        fencingToken: null,
        hlc: null,
        candyCount: null,
        sizeBytes: null,
      }),
    ).not.toThrow();
  });

  it('ClusterSchema accepts nodes with null optional fields', () => {
    expect(() =>
      ClusterSchema.parse({
        nodes: [{ nodeId: 'n1', address: null, ready: null, ownedBoxCount: null }],
        boxCount: 0,
        ownerless: [],
      }),
    ).not.toThrow();
  });

  it('CandyListingSchema accepts null nextStartAfter', () => {
    expect(() =>
      CandyListingSchema.parse({ entries: [], nextStartAfter: null }),
    ).not.toThrow();
  });

  it('mutating helpers wire URLs, methods, and bodies through fetch', async () => {
    const calls: Array<{ url: string; init: RequestInit }> = [];
    const fetchMock = vi.fn(async (url: string, init: RequestInit) => {
      calls.push({ url, init });
      return new Response(null, { status: 204 });
    });
    vi.stubGlobal('fetch', fetchMock);

    await createBox('sweets');
    await deleteBox('sweets', true);
    // Key with a slash must keep the slash unescaped so the admin API routes it as a sub-path.
    await uploadCandy('photos', 'album/cat.jpg',
      new File([new Uint8Array([1, 2, 3])], 'cat.jpg', { type: 'image/jpeg' }));
    await deleteCandy('photos', 'album/cat.jpg');

    expect(calls.map((c) => `${c.init.method} ${c.url}`)).toEqual([
      'POST /api/boxes',
      'DELETE /api/boxes/sweets?force=true',
      'PUT /api/boxes/photos/objects/album/cat.jpg',
      'DELETE /api/boxes/photos/objects/album/cat.jpg',
    ]);
    const headers = calls[0].init.headers as Record<string, string>;
    expect(headers['Content-Type']).toBe('application/json');
    expect(calls[0].init.body).toBe('{"name":"sweets"}');
    const uploadHeaders = calls[2].init.headers as Record<string, string>;
    expect(uploadHeaders['Content-Type']).toBe('image/jpeg');
  });

  it('apiSend decodes admin-API error envelopes into ApiError.code/detail', async () => {
    vi.stubGlobal('fetch', async () =>
      new Response(JSON.stringify({ error: 'Conflict', message: 'box already exists: sweets' }), {
        status: 409,
        headers: { 'Content-Type': 'application/json' },
      }),
    );
    await expect(createBox('sweets')).rejects.toMatchObject({
      status: 409,
      code: 'Conflict',
      detail: 'box already exists: sweets',
    });
    await expect(createBox('sweets')).rejects.toBeInstanceOf(ApiError);
  });

  it('LsmSchema accepts boxes with all-null runtime fields', () => {
    expect(() =>
      LsmSchema.parse({
        boxes: [
          {
            box: 'photos',
            owner: null,
            manifestVersion: null,
            sstableLedgerCount: null,
            syrupLedgerCount: null,
            walLedgerCount: null,
            inFlightCompactions: null,
            gcBacklog: null,
          },
        ],
      }),
    ).not.toThrow();
  });
});
