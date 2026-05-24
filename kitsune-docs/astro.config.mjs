// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
	integrations: [
		starlight({
			title: 'Kitsune Docs',
			description: 'Architecture, usage, components, and roadmap for the Kitsune distributed search engine.',
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
