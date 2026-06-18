# Candybox documentation site

The Candybox user manual / project website. It is built with [Hugo](https://gohugo.io) and the
[hugo-book](https://github.com/alex-shpak/hugo-book) theme, and published to GitHub Pages at
**https://predatorray.github.io/candybox/** by the
[`docs.yml`](../.github/workflows/docs.yml) GitHub Actions workflow on every push to `main` that
touches `docs/`.

## Layout

```
docs/
├── config.toml              # Hugo site configuration
├── content/
│   ├── _index.md            # landing page (https://predatorray.github.io/candybox/)
│   └── docs/                # the manual; this section becomes the left sidebar
│       ├── getting-started/
│       ├── installation/
│       ├── architecture/
│       ├── client/
│       ├── s3-gateway/
│       ├── operations/
│       └── reference/
└── themes/
    └── hugo-book/           # theme, vendored as a git submodule
```

To add a page, drop a markdown file (with a `title` and `weight` front matter) into the right folder
under `content/docs/`. The sidebar order follows `weight`.

## Prerequisites

- **Hugo extended**, version **0.158.0 or newer** (the theme requires it). Install from
  <https://gohugo.io/installation/>.
- The theme submodule. After cloning the repo:

  ```bash
  git submodule update --init docs/themes/hugo-book
  ```

  (A fresh clone can also use `git clone --recurse-submodules`.)

## Preview locally

```bash
cd docs
hugo server          # live-reloading preview at http://localhost:1313/candybox/
```

## Build

```bash
cd docs
hugo --gc --minify   # output written to docs/public/
```

`docs/public/` is generated output and is git-ignored.
