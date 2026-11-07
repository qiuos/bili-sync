<script module lang="ts">
	import ImageOffIcon from '@lucide/svelte/icons/image-off';
	import { acquireCover, releaseCover } from '$lib/cover-cache';
	import { cn } from '$lib/utils';
</script>

<script lang="ts">
	let {
		videoId,
		alt,
		class: className
	}: { videoId: number; alt: string; class?: string } = $props();

	let url = $state<string | null>(null);
	let status = $state<'loading' | 'ready' | 'missing'>('loading');

	$effect(() => {
		const id = videoId;
		let released = false;
		status = 'loading';
		url = null;

		const { url: cachedUrl, pending } = acquireCover(id);
		const settle = (objectUrl: string | null) => {
			if (released) return;
			if (objectUrl) {
				url = objectUrl;
				status = 'ready';
			} else {
				status = 'missing';
			}
		};
		if (cachedUrl) {
			settle(cachedUrl);
		} else {
			pending?.then(settle);
		}

		return () => {
			released = true;
			releaseCover(id);
		};
	});
</script>

<div
	class={cn('bg-muted relative aspect-video overflow-hidden', className)}
	role="img"
	aria-label={status === 'ready' ? alt : '暂无本地封面'}
>
	{#if status === 'ready' && url}
		<img src={url} {alt} class="h-full w-full object-cover" loading="lazy" />
	{:else if status === 'missing'}
		<div
			class="text-muted-foreground/50 flex h-full w-full flex-col items-center justify-center gap-1.5"
		>
			<ImageOffIcon class="h-6 w-6" aria-hidden="true" />
			<span class="text-xs">暂无本地封面</span>
		</div>
	{:else}
		<div class="animate-pulse bg-muted-foreground/10 h-full w-full"></div>
	{/if}
</div>
