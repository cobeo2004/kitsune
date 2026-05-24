// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import sitemap from '@astrojs/sitemap';
import vercel from '@astrojs/vercel';
const site = process.env.VERCEL_URL ?? "http://localhost:4321";

// https://astro.build/config
export default defineConfig({
	site,
	output: "static",
	prefetch: {
		prefetchAll: true,
		defaultStrategy: "hover"
	},
	adapter: vercel({
		imageService: true,
		devImageService: "sharp"
	}),
	integrations: [
		sitemap(),
		starlight({
			title: 'Kitsune Docs',
			description: 'Architecture, usage, components, and roadmap for the Kitsune distributed search engine.',
			logo: {
				src: './src/assets/kitsune-logo.png',
				alt: 'Kitsune',
				replacesTitle: false,
			},
			customCss: [
				'./src/styles/fonts.css',
				'./src/styles/tokens.css',
				'./src/styles/starlight-overrides.css',
				'./src/styles/brand.css',
			],
			components: {
				SiteTitle: './src/components/SiteTitle.astro',
				Hero: './src/components/Hero.astro',
				PageTitle: './src/components/PageTitle.astro',
				Footer: './src/components/Footer.astro',
			},
			social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/cobeo2004/kitsune' }],
			sidebar: [
				{
					label: 'Kitsune',
					items: [
						{ label: 'Overview', slug: '' },
						{ label: 'Architecture', slug: 'architecture' },
						{ label: 'Usage', slug: 'usage' },
						{ label: 'Components', slug: 'components' },
						{ label: 'Technical Decisions', slug: 'technical-decisions' },
						{ label: 'Roadmap', slug: 'roadmap' },
					],
				},
			],
		}),
	],
});
