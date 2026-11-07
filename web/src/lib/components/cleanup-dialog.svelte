<script lang="ts">
	import { Button } from '$lib/components/ui/button/index.js';
	import LoaderCircleIcon from '@lucide/svelte/icons/loader-circle';
	import TriangleAlertIcon from '@lucide/svelte/icons/triangle-alert';
	import BrushCleaningIcon from '@lucide/svelte/icons/brush-cleaning';
	import {
		Dialog,
		DialogContent,
		DialogDescription,
		DialogFooter,
		DialogHeader,
		DialogTitle
	} from '$lib/components/ui/dialog/index.js';
	import api from '$lib/api';
	import type { ApiError, VideosCleanupPreviewResponse, VideosCleanupRequest } from '$lib/types';
	import { toast } from 'svelte-sonner';

	interface Props {
		open: boolean;
		// 清理完成后由父组件刷新列表
		onFinished: (() => void) | null;
	}

	let { open = $bindable(false), onFinished = null }: Props = $props();

	type Phase = 'loading' | 'preview' | 'executing' | 'done';

	let phase = $state<Phase>('loading');
	let loadError = $state('');
	let preview = $state<VideosCleanupPreviewResponse | null>(null);
	let result = $state<{
		deleted_videos_count: number;
		deleted_pages_count: number;
		skipped_count: number;
	} | null>(null);

	// 本次对话框会话的序号：关闭后重新打开自增，用于丢弃上一轮的过期异步结果
	let session = $state(0);

	let totalCount = $derived(preview ? preview.invalid_ids.length + preview.missing_ids.length : 0);
	let skippedSourcePaths = $derived(preview?.skipped_source_paths ?? []);

	// 执行中禁止 Esc / 点击遮罩关闭，防止半途关闭造成状态不可知
	function blockCloseDuringExecute(event: Event) {
		if (phase === 'executing') {
			event.preventDefault();
		}
	}

	$effect(() => {
		if (open) {
			loadPreview();
		} else {
			phase = 'loading';
			loadError = '';
			preview = null;
			result = null;
		}
	});

	// 关闭或重新打开后丢弃本轮结果：异步完成时校验 open 且会话一致才写状态
	function isStale(asyncSession: number): boolean {
		return !open || asyncSession !== session;
	}

	async function loadPreview() {
		const asyncSession = session;
		phase = 'loading';
		loadError = '';
		preview = null;
		result = null;
		try {
			const response = await api.previewVideosCleanup();
			if (isStale(asyncSession)) return;
			preview = response.data;
			phase = 'preview';
		} catch (error) {
			if (isStale(asyncSession)) return;
			console.error('获取清理预览失败：', error);
			loadError = (error as ApiError).message;
			phase = 'preview';
		}
	}

	async function handleExecute() {
		if (!preview) return;
		const request: VideosCleanupRequest = {
			// 原样提交预览确认过的候选 ID；后端执行时二次核验，实际数量以结果为准
			invalid_ids: preview.invalid_ids,
			missing_ids: preview.missing_ids
		};
		const asyncSession = session;
		phase = 'executing';
		try {
			const response = await api.executeVideosCleanup(request);
			// 列表刷新与通知无论对话框是否仍打开都要完成
			toast.success('清理完成', {
				description:
					response.data.skipped_count > 0
						? `已删除 ${response.data.deleted_videos_count} 条视频记录和 ${response.data.deleted_pages_count} 条分P记录，${response.data.skipped_count} 条记录状态变化被跳过`
						: `已删除 ${response.data.deleted_videos_count} 条视频记录和 ${response.data.deleted_pages_count} 条分P记录，本地文件未受影响`
			});
			onFinished?.();
			if (isStale(asyncSession)) return;
			result = response.data;
			phase = 'done';
		} catch (error) {
			if (isStale(asyncSession)) return;
			console.error('执行清理失败：', error);
			toast.error('执行清理失败', { description: (error as ApiError).message });
			phase = 'preview';
		}
	}
</script>

<Dialog bind:open>
	<DialogContent
		class="sm:max-w-md"
		onEscapeKeydown={blockCloseDuringExecute}
		onInteractOutside={blockCloseDuringExecute}
	>
		<DialogHeader>
			<DialogTitle class="flex items-center gap-2">
				<BrushCleaningIcon class="h-5 w-5" aria-hidden="true" />
				清理失效记录
			</DialogTitle>
			<DialogDescription>
				批量删除数据库中的记录以保持列表整洁，<span class="text-foreground font-medium"
					>绝不删除本地视频文件</span
				>
			</DialogDescription>
		</DialogHeader>

		{#if phase === 'loading'}
			<div class="text-muted-foreground/70 flex items-center justify-center gap-2 py-8">
				<LoaderCircleIcon class="h-5 w-5 animate-spin" aria-hidden="true" />
				正在统计可清理的记录...
			</div>
		{:else if loadError}
			<div
				role="alert"
				class="text-destructive bg-destructive/10 flex flex-col items-center justify-center gap-3 rounded-lg p-6 text-sm"
			>
				<span>{loadError}</span>
				<Button variant="outline" class="cursor-pointer" onclick={loadPreview}>重试</Button>
			</div>
		{:else if phase === 'done' && result}
			{#if result.deleted_videos_count === 0 && result.deleted_pages_count === 0}
				<div class="flex flex-col items-center justify-center gap-2 py-8">
					<BrushCleaningIcon class="text-muted-foreground/40 h-8 w-8" aria-hidden="true" />
					<p class="text-muted-foreground text-sm">没有记录被清理</p>
					{#if result.skipped_count > 0}
						<p class="text-muted-foreground/70 text-xs">
							{result.skipped_count} 条记录因状态变化或安全策略被跳过
						</p>
					{/if}
				</div>
			{:else}
				<div role="status" class="flex flex-col items-center gap-2 py-6 text-sm">
					<p>
						已删除 <strong>{result.deleted_videos_count}</strong> 条视频记录和
						<strong>{result.deleted_pages_count}</strong> 条分P记录
					</p>
					{#if result.skipped_count > 0}
						<p class="text-muted-foreground text-xs">
							{result.skipped_count} 条记录因状态变化或安全策略被跳过，未清理
						</p>
					{/if}
					<p class="text-muted-foreground/70 text-xs">本地视频文件未受任何影响</p>
				</div>
			{/if}
		{:else if preview}
			{#if skippedSourcePaths.length > 0}
				<div
					role="status"
					class="mb-3 rounded-lg border border-amber-200 bg-amber-50 p-3 dark:border-amber-900 dark:bg-amber-950/40"
				>
					<div
						class="mb-1.5 flex items-center gap-2 text-sm font-medium text-amber-700 dark:text-amber-300"
					>
						<TriangleAlertIcon class="h-4 w-4 shrink-0" aria-hidden="true" />
						以下视频源目录未被检查本地文件
					</div>
					<p class="text-xs leading-relaxed text-amber-700 dark:text-amber-300">
						无法确认这些目录下仍有成功下载的视频文件，其下的记录不会按「本地文件全缺失」清理（防止磁盘未挂载或路径错误导致误判）：
					</p>
					<ul class="mt-1.5 space-y-1 font-mono text-xs text-amber-700 dark:text-amber-300">
						{#each skippedSourcePaths as path (path)}
							<li class="truncate" title={path}>{path}</li>
						{/each}
					</ul>
				</div>
			{/if}
			{#if totalCount === 0}
				<div class="flex flex-col items-center justify-center gap-2 py-8">
					<BrushCleaningIcon class="text-muted-foreground/40 h-8 w-8" aria-hidden="true" />
					<p class="text-muted-foreground text-sm">当前没有可清理的记录</p>
				</div>
			{:else}
				<div class="space-y-3">
					<div class="rounded-lg border bg-muted/30 p-3">
						<div class="text-foreground mb-2 text-sm font-medium">将清理以下两类记录：</div>
						<ul class="space-y-1.5 text-sm">
							<li class="flex items-center justify-between gap-2">
								<span class="min-w-0">B 站失效视频（B 站已无法访问）</span>
								<strong class="shrink-0">{preview.invalid_ids.length} 条</strong>
							</li>
							<li class="flex items-start justify-between gap-2">
								<span class="min-w-0">已下载完成但全部分P视频文件均不存在</span>
								<strong class="shrink-0">{preview.missing_ids.length} 条</strong>
							</li>
						</ul>
						<div class="text-muted-foreground mt-2 border-t pt-2 text-sm">
							共 <strong class="text-foreground">{totalCount}</strong> 条记录
						</div>
					</div>
					<div
						role="status"
						class="rounded-lg border border-amber-200 bg-amber-50 p-3 dark:border-amber-900 dark:bg-amber-950/40"
					>
						<div
							class="mb-1.5 flex items-center gap-2 text-sm font-medium text-amber-700 dark:text-amber-300"
						>
							<TriangleAlertIcon class="h-4 w-4 shrink-0" aria-hidden="true" />
							执行前的注意事项
						</div>
						<ul class="space-y-1 text-xs leading-relaxed text-amber-700 dark:text-amber-300">
							<li>
								执行时会重新核验每条记录，状态已变化的记录将被跳过，实际清理数量以结果为准；部分文件缺失、源目录不存在或不可访问的记录不会被清理。
							</li>
							<li>
								已下载但文件缺失的记录删除后，重新扫描有机会重新发现并下载；B
								站失效记录删除后不保证重新扫描可发现。
							</li>
						</ul>
					</div>
				</div>
			{/if}
		{/if}

		<DialogFooter>
			{#if (phase === 'preview' || phase === 'executing') && preview && totalCount > 0}
				<Button
					variant="outline"
					class="cursor-pointer"
					onclick={loadPreview}
					disabled={phase === 'executing'}>重新统计</Button
				>
				<Button
					class="bg-destructive hover:bg-destructive/90 cursor-pointer"
					onclick={handleExecute}
					disabled={phase === 'executing'}
				>
					{#if phase === 'executing'}
						<LoaderCircleIcon class="mr-1.5 h-4 w-4 animate-spin" aria-hidden="true" />
						清理中...
					{:else}
						确认清理
					{/if}
				</Button>
			{:else}
				<Button variant="outline" class="cursor-pointer" onclick={() => (open = false)}>
					关闭
				</Button>
			{/if}
		</DialogFooter>
	</DialogContent>
</Dialog>
