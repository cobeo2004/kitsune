# Kitsune Docs

[![Built with Starlight](https://astro.badg.es/v2/built-with-starlight/tiny.svg)](https://starlight.astro.build)

This is the Astro Starlight documentation site for Kitsune.

The source pages live in `src/content/docs`. Each page uses Starlight-compatible frontmatter with `title`, `description`, and `sidebar` metadata so the docs can be reorganized or published without rewriting content.

## Commands

Run commands from `kitsune-docs`:

| Command                   | Action                                           |
| :------------------------ | :----------------------------------------------- |
| `npm install`             | Installs dependencies                            |
| `npm run dev`             | Starts local dev server at `localhost:4321`      |
| `npm run build`           | Build your production site to `./dist/`          |
| `npm run preview`         | Preview your build locally, before deploying     |
| `npm run astro ...`       | Run CLI commands like `astro add`, `astro check` |
| `npm run astro -- --help` | Get help using the Astro CLI                     |

## Content Map

- `src/content/docs/index.md`: project overview.
- `src/content/docs/architecture.md`: distributed architecture and request flows.
- `src/content/docs/usage.md`: local development and API usage.
- `src/content/docs/components.md`: component responsibilities.
- `src/content/docs/technical-decisions.md`: durable design decisions and rejected alternatives.
- `src/content/docs/roadmap.md`: milestone and future roadmap.

Static images used by the docs live in `public/assets`.
