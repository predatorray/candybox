---
title: Command-line client
weight: 40
---

# Command-line client

The `candybox` CLI is bundled in the same image as the storage node (the image is dual-mode). Point it
at a node with the `CANDYBOX_SERVER` environment variable or the `-s host:port` flag.

The same operations are available programmatically through the `CandyboxClient` class in the
`candybox-client` module — see [installation]({{< relref "/docs/installation" >}}).

## Basic object operations

```bash
candybox create-box photos
candybox put  photos cat.jpg cat.jpg --content-type image/jpeg
candybox get  photos cat.jpg out.jpg
candybox head photos cat.jpg            # size, content-type, checksum, metadata
candybox list photos                    # keys in the box
candybox list-boxes
candybox help                           # full command list
```

`put` reads from a file or, if you omit the path, from standard input; `get` writes to a file or to
standard output.

## Operations the sorted LSM tree makes cheap

```bash
candybox list   photos --start a --end m --reverse   # bounded, reverse-order range scan
candybox copy   photos cat.jpg cat-copy.jpg          # zero-copy: shares the stored bytes
candybox rename photos cat.jpg pets/cat.jpg          # zero-copy move (within a Box)
candybox delete-range photos thumbnails/             # one O(1) range tombstone, not N deletes
candybox delete-range photos --start a --end m       # delete a half-open [start, end) key window
```

- **Bounded / reverse range scans** walk a `[start, end)` window in either direction (`list --start K
  --end K --reverse`), paging with `--start-after`.
- **Zero-copy `copy` / `rename`** point a new key at the *same* stored bytes — no data is moved, even
  when source and destination land in different hash partitions (the stored bytes are shared across
  partitions). A same-partition `rename` removes the source atomically; a cross-partition `rename` is
  *eventually* atomic — it converges to "source gone, destination present" (a reader may briefly see
  both keys, but the rename never strands both keys forever).
- **`delete-range`** deletes a whole prefix or key window with a single range tombstone (constant work
  regardless of how many keys it covers); the bytes are reclaimed lazily by compaction.

## Range GET

Object reads accept HTTP-style ranges through the gateway and client: `bytes=A-B`, `bytes=A-`, and
`bytes=-N`, returning the requested window. Multi-range requests are rejected. See the
[S3 gateway]({{< relref "/docs/s3-gateway" >}}) for the HTTP surface.
