<script lang="ts">
	import { Button } from '$lib/components/ui/button/index.js';
	import { Badge } from '$lib/components/ui/badge/index.js';
	import { Checkbox } from '$lib/components/ui/checkbox/index.js';
	import LoaderCircleIcon from '@lucide/svelte/icons/loader-circle';
	import TriangleAlertIcon from '@lucide/svelte/icons/triangle-alert';
	import ListChecksIcon from '@lucide/svelte/icons/list-checks';
	import {
		Dialog,
		DialogContent,
		DialogDescription,
		DialogFooter,
		DialogHeader,
		DialogTitle
	} from '$lib/components/ui/dialog/index.js';
	import { toast } from 'svelte-sonner';
	import { SvelteSet } from 'svelte/reactivity';
	import api from '$lib/api';
	import type {
		ApiError,
		OrganizationExecuteResponse,
		OrganizationItemAction,
		OrganizationPlanItem,
		OrganizationRule
	} from '$lib/types';

	interface Props {
		open: boolean;
		rule: OrganizationRule | null;
		onFinished: (() => void) | null;
	}

	let { open = $bindable(false), rule = null, onFinished = null }: Props = $props();

	type Phase = 'loading' | 'preview' | 'executing' | 'done';

	const ACTION_META: Record<OrganizationItemAction, { label: string; badgeClass: string }> = {
		create: {
			label: '新建',
			badgeClass: 'bg-emerald-100 text-emerald-700 dark:bg-emerald-950 dark:text-emerald-300'
		},
		refresh: {
			label: '刷新',
			badgeClass: 'bg-sky-100 text-sky-700 dark:bg-sky-950 dark:text-sky-300'
		},
		exists: {
			label: '已存在',
			badgeClass: 'bg-muted text-muted-foreground'
		},
		conflict: {
			label: '冲突',
			badgeClass: 'bg-amber-100 text-amber-700 dark:bg-amber-950 dark:text-amber-300'
		},
		missing: {
			label: '缺文件',
			badgeClass: 'bg-muted text-muted-foreground'
		},
		failed: {
			label: '失败',
			badgeClass: 'bg-rose-100 text-rose-700 dark:bg-rose-950 dark:text-rose-300'
		}
	};

	let phase = $state<Phase>('loading');
	let loadError = $state('');
	let items = $state<OrganizationPlanItem[]>([]);
	let warnings = $state<string[]>([]);
	let result = $state<OrganizationExecuteResponse | null>(null);
	// 电影规则按视频勾选：选中一个视频即包含其全部版本，execute 发送 video_ids
	const selectedVideos = new SvelteSet<number>();

	let isMovie = $derived(rule?.rule_type === 'movie');

	// 电视剧按剧集分组保持后端排序
	let seriesGroups = $derived.by(() => {
		if (!isMovie) {
			const groups: { series: string; items: OrganizationPlanItem[] }[] = [];
			// eslint-disable-next-line svelte/prefer-svelte-reactivity -- 纯派生计算的局部累加器，无需响应式
			const index = new Map<string, number>();
			for (const item of items) {
				const series = item.series ?? '未分组';
				let groupIndex = index.get(series);
				if (groupIndex === undefined) {
					groupIndex = groups.length;
					index.set(series, groupIndex);
					groups.push({ series, items: [] });
				}
				groups[groupIndex].items.push(item);
			}
			return groups;
		}
		return null;
	});

	// 电影按 video_id 聚合，一个视频一组版本
	let movieGroups = $derived.by(() => {
		if (!isMovie) return null;
		const groups: {
			videoId: number;
			videoName: string;
			items: OrganizationPlanItem[];
			hasVersion: boolean;
			executable: boolean;
		}[] = [];
		// eslint-disable-next-line svelte/prefer-svelte-reactivity -- 纯派生计算的局部累加器，无需响应式
		const index = new Map<number, number>();
		for (const item of items) {
			let groupIndex = index.get(item.video_id);
			if (groupIndex === undefined) {
				groupIndex = groups.length;
				index.set(item.video_id, groupIndex);
				groups.push({
					videoId: item.video_id,
					videoName: item.video_name,
					items: [],
					hasVersion: false,
					executable: false
				});
			}
			groups[groupIndex].items.push(item);
		}
		for (const group of groups) {
			group.hasVersion = group.items.length > 1;
			group.executable = group.items.some(
				(item) => item.action === 'create' || item.action === 'refresh'
			);
		}
		return groups;
	});

	// 电视剧按可执行项计数；电影按勾选且可执行的视频计数
	let executableCount = $derived.by(() => {
		if (isMovie) {
			return (
				movieGroups?.filter((group) => group.executable && selectedVideos.has(group.videoId))
					.length ?? 0
			);
		}
		return items.filter((item) => item.action === 'create' || item.action === 'refresh').length;
	});

	$effect(() => {
		if (open) {
			loadPreview();
		} else {
			phase = 'loading';
			loadError = '';
			items = [];
			warnings = [];
			result = null;
			selectedVideos.clear();
		}
	});

	async function loadPreview() {
		phase = 'loading';
		loadError = '';
		items = [];
		warnings = [];
		result = null;
		selectedVideos.clear();
		if (!rule) return;
		try {
			// 电影规则不传 video_ids 时后端返回源目录内全部候选；execute 时仍要求显式选择
			const response = await api.organization.previewRule(rule.id);
			items = response.data.items;
			warnings = response.data.warnings;
			phase = 'preview';
		} catch (error) {
			console.error('生成整理预览失败：', error);
			loadError = (error as ApiError).message;
			phase = 'preview';
		}
	}

	async function handleExecute() {
		if (!rule) return;
		phase = 'executing';
		try {
			const response = await api.organization.executeRule(
				rule.id,
				isMovie ? [...selectedVideos] : undefined
			);
			result = response.data;
			items = response.data.items;
			warnings = response.data.warnings;
			phase = 'done';
			toast.success('整理完成', {
				description: `新建 ${response.data.created}，刷新 ${response.data.refreshed}，跳过 ${response.data.skipped}，失败 ${response.data.failed}`
			});
			onFinished?.();
		} catch (error) {
			console.error('执行整理失败：', error);
			toast.error('执行整理失败', { description: (error as ApiError).message });
			phase = 'preview';
		}
	}
</script>

<Dialog bind:open>
	<DialogContent class="flex h-[85vh] max-h-[85vh] flex-col overflow-hidden sm:max-w-3xl">
		<DialogHeader class="shrink-0">
			<DialogTitle class="flex items-center gap-2">
				<ListChecksIcon class="h-5 w-5" aria-hidden="true" />
				整理预览「{rule?.name ?? ''}」
			</DialogTitle>
			<DialogDescription>
				{#if isMovie}
					源目录内的候选电影，勾选后执行；多P视频视为同一电影的多个版本
				{:else}
					源目录及子目录内的视频聚合为一部剧，源目录名称作为剧名，每个分P一集
				{/if}
				· 硬链接与源文件共享内容，修改任一文件会影响另一处；整理期间请勿移动或替换目录、变更挂载
			</DialogDescription>
		</DialogHeader>

		{#if phase === 'loading'}
			<div class="text-muted-foreground/70 flex flex-1 items-center justify-center gap-2">
				<LoaderCircleIcon class="h-5 w-5 animate-spin" aria-hidden="true" />
				正在生成整理计划...
			</div>
		{:else if loadError}
			<div
				role="alert"
				class="text-destructive bg-destructive/10 flex flex-1 flex-col items-center justify-center gap-3 rounded-lg p-6 text-sm"
			>
				<span>{loadError}</span>
				<Button variant="outline" class="cursor-pointer" onclick={loadPreview}>重试</Button>
			</div>
		{:else if items.length === 0}
			<div class="flex flex-1 items-center justify-center">
				<div class="space-y-3 text-center">
					<p class="text-muted-foreground text-sm">没有可整理的内容</p>
					<p class="text-muted-foreground/70 text-xs">
						源目录内没有已下载的视频，或视频均未下载完成
					</p>
				</div>
			</div>
		{:else}
			{#if warnings.length > 0}
				<div
					role="status"
					class="shrink-0 rounded-lg border border-amber-200 bg-amber-50 p-3 dark:border-amber-900 dark:bg-amber-950/40"
				>
					<div
						class="mb-1.5 flex items-center gap-2 text-sm font-medium text-amber-700 dark:text-amber-300"
					>
						<TriangleAlertIcon class="h-4 w-4" aria-hidden="true" />
						{warnings.length} 条提示
					</div>
					<ul class="space-y-1 text-xs leading-relaxed text-amber-700 dark:text-amber-300">
						{#each warnings as warning (warning)}
							<li>{warning}</li>
						{/each}
					</ul>
				</div>
			{/if}

			{#if result}
				<div class="text-muted-foreground flex shrink-0 flex-wrap gap-x-5 gap-y-1 text-sm">
					<span>新建 <strong class="text-foreground">{result.created}</strong></span>
					<span>刷新 <strong class="text-foreground">{result.refreshed}</strong></span>
					<span>跳过 <strong class="text-foreground">{result.skipped}</strong></span>
					<span>失败 <strong class="text-foreground">{result.failed}</strong></span>
					<span>附加文件 <strong class="text-foreground">{result.extras_created}</strong></span>
				</div>
			{/if}

			<div class="min-h-0 flex-1 overflow-y-auto">
				{#if seriesGroups}
					{#each seriesGroups as group (group.series)}
						<section class="mb-5">
							<h3 class="text-foreground mb-2 text-sm font-semibold">
								{group.series}
								<span class="text-muted-foreground ml-2 font-normal">{group.items.length} 集</span>
							</h3>
							<ul class="space-y-1.5">
								{#each group.items as item (item.page_id)}
									{@render PlanItemRow(item)}
								{/each}
							</ul>
						</section>
					{/each}
				{:else if movieGroups}
					<ul class="space-y-2">
						{#each movieGroups as group (group.videoId)}
							<li class="bg-muted/30 rounded-lg border px-3 py-2.5">
								<div class="flex items-center gap-2">
									{#if group.executable}
										<Checkbox
											checked={selectedVideos.has(group.videoId)}
											onCheckedChange={(checked) =>
												checked === true
													? selectedVideos.add(group.videoId)
													: selectedVideos.delete(group.videoId)}
											aria-label={`选择电影 ${group.videoName}`}
										/>
									{/if}
									<span class="truncate text-sm font-semibold">{group.videoName}</span>
									{#if group.hasVersion}
										<Badge variant="outline" class="shrink-0 rounded-full px-2 text-xs">
											{group.items.length} 个版本
										</Badge>
									{/if}
								</div>
								<ul class="mt-1.5 space-y-1.5">
									{#each group.items as item (item.page_id)}
										{@render PlanItemRow(item)}
									{/each}
								</ul>
							</li>
						{/each}
					</ul>
				{/if}
			</div>

			<DialogFooter class="shrink-0">
				{#if phase === 'preview' || phase === 'executing'}
					<Button
						variant="outline"
						class="cursor-pointer"
						onclick={loadPreview}
						disabled={phase === 'executing'}
					>
						重新预览
					</Button>
					<Button
						class="cursor-pointer"
						onclick={handleExecute}
						disabled={phase === 'executing' || executableCount === 0}
					>
						{#if phase === 'executing'}
							<LoaderCircleIcon class="mr-1.5 h-4 w-4 animate-spin" aria-hidden="true" />
							执行中...
						{:else}
							执行整理（{executableCount}{isMovie ? ' 部' : ''}）
						{/if}
					</Button>
				{:else}
					<Button variant="outline" class="cursor-pointer" onclick={() => (open = false)}>
						关闭
					</Button>
				{/if}
			</DialogFooter>
		{/if}
	</DialogContent>
</Dialog>

{#snippet PlanItemRow(item: OrganizationPlanItem)}
	{@const meta = ACTION_META[item.action]}
	<li class="rounded-lg border px-3 py-2">
		<div class="flex items-center gap-2">
			<Badge class={`${meta.badgeClass} rounded-full border-transparent px-2 text-xs font-medium`}>
				{meta.label}
			</Badge>
			{#if item.episode !== null}
				<Badge variant="outline" class="rounded-full px-2 text-xs">E{item.episode}</Badge>
			{/if}
			<span class="truncate text-sm font-medium">{item.page_name}</span>
			{#if item.video_name !== item.page_name}
				<span class="text-muted-foreground truncate text-xs">{item.video_name}</span>
			{/if}
		</div>
		{#if item.target}
			<p class="text-muted-foreground mt-1 truncate font-mono text-xs" title={item.target}>
				{item.target}
			</p>
		{/if}
		{#if item.message}
			<p
				class="mt-1 text-xs leading-relaxed {item.action === 'failed' || item.action === 'conflict'
					? 'text-destructive'
					: 'text-muted-foreground'}"
			>
				{item.message}
			</p>
		{/if}
	</li>
{/snippet}
