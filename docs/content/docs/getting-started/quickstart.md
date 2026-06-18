---
title: Quick start
weight: 1
---

# Quick start

The fastest way to try Candybox is the bundled Docker Compose stack. It starts the full system —
ZooKeeper, 3 BookKeeper bookies, 3 Candybox nodes, and the S3 gateway (on `:9711`) — using the
published [`zetaplusae/candybox`](https://hub.docker.com/r/zetaplusae/candybox) image.

## With Docker Compose (recommended)

From a checkout of the [repository](https://github.com/predatorray/candybox):

```bash
docker compose up -d
docker compose run --rm cli create-box photos
echo 'hello candybox' | docker compose run --rm -T cli put photos hello.txt
docker compose run --rm cli get photos hello.txt   # -> hello candybox
```

Tear it down with `docker compose down` (add `-v` to also wipe the data volumes).

### Web dashboard

The compose stack also brings up a stateless **admin / dashboard service** at
[`http://localhost:9713/ui/`](http://localhost:9713/ui/) — a React + TypeScript + MUI single-page app
that shows cluster topology, the box browser, LSM internals (manifest version + fencing token), and a
small set of time-series charts polled from each node's `/metrics`. The same process exposes a JSON
API at `/api/*` (see [Operations]({{< relref "/docs/operations" >}})).

## Storing and retrieving objects

The `zetaplusae/candybox` image is dual-mode: passing `candybox <args>` runs the command-line client
instead of a storage node. Point it at a node with `CANDYBOX_SERVER` (or `-s host:port`); to reach the
cluster started above, join its Compose network (`candybox_default` by default) and mount a directory
to exchange files. An alias keeps the commands readable:

```bash
alias candybox='docker run --rm -i --network candybox_default \
  -e CANDYBOX_SERVER=candybox-1:9709 -v "$PWD:/data" -w /data zetaplusae/candybox candybox'

candybox create-box photos
candybox put  photos cat.jpg cat.jpg --content-type image/jpeg
candybox get  photos cat.jpg out.jpg
candybox head photos cat.jpg            # size, content-type, checksum, metadata
candybox list photos                    # keys in the box
candybox list-boxes
candybox help                           # full command list
```

See the [command-line client]({{< relref "/docs/client" >}}) for the full command set, including the
range scans, zero-copy `copy` / `rename`, and `delete-range` that the sorted LSM tree makes cheap.

## Next steps

- Read the [concepts]({{< relref "concepts" >}}) to understand Boxes, Candy, and Syrups.
- Talk to Candybox over S3 SDKs via the [S3 gateway]({{< relref "/docs/s3-gateway" >}}).
- Run it for real: see [installation]({{< relref "/docs/installation" >}}) and
  [operations]({{< relref "/docs/operations" >}}).
