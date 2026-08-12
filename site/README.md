# site/ — tandem.codingful.com

The project's landing page and the dereferenceable **problem-type pages** for the Admin API.

Hand-written HTML and one stylesheet: no framework, no generator, no build dependency beyond
`bash` and `cp`. That is deliberate — the page is a shop window, not a documentation portal, and a
toolchain here would be a second thing to maintain for no reader benefit.

## Layout

| Path | What it is |
|---|---|
| `index.html` | The landing page. |
| `problems/index.html` | Index of every RFC 9457 problem type the Admin API returns. |
| `problems/<slug>/index.html` | One page per problem type — the `type` URI resolves here. |
| `privacy/index.html` | Privacy notice — linked from every page's footer. |
| `404.html` | Served by GitHub Pages for unknown paths. |
| `assets/style.css` | The whole stylesheet. Palette taken from the brand SVGs in `docs/`. |
| `CNAME` | The custom domain. Removing it moves the site to `alirux.github.io/tandem`. |
| `build.sh` | Assembles `site/_build` (gitignored) — copies this directory, then the images. |

**The images are not in here.** `build.sh` copies them from `docs/`, where the README already
references them, so a redrawn diagram updates both places at once instead of drifting.

## Preview locally

```bash
./site/build.sh && python3 -m http.server -d site/_build 8000
```

Then open <http://localhost:8000>. Serving `_build` (rather than opening `index.html` from disk) is
what makes the clean URLs — `/problems/not-found/` — resolve the way GitHub Pages resolves them.

## Deploy

`.github/workflows/pages.yml` runs `build.sh` and deploys to GitHub Pages on every push to `main`
that touches `site/**` or one of the copied images, and on manual dispatch.

## The rule that keeps this from rotting

**The site cites, it does not re-document.** Feature prose, the module list, usage guides and the
design documents live in `README.md` and `docs/`; the page links to them. The one thing it owns
outright is the problem-type pages, because those URLs are part of the published API contract
(`docs/admin-api.openapi.yaml`) and have nowhere else to live.

Adding a problem type to the contract therefore means adding a page here, in the same change: a
`type` URL that 404s is a broken contract, not a missing nicety.
