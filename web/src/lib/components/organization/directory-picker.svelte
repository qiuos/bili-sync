<script lang="ts">
	import { untrack } from 'svelte';
	import { Button } from '$lib/components/ui/button/index.js';
	import LoaderCircleIcon from '@lucide/svelte/icons/loader-circle';
	import CornerLeftUpIcon from '@lucide/svelte/icons/corner-left-up';
	import FolderIcon from '@lucide/svelte/icons/folder';
	import FolderOpenIcon from '@lucide/svelte/icons/folder-open';
	import {
		Dialog,
		DialogContent,
		DialogDescription,
		DialogFooter,
		DialogHeader,
		DialogTitle
	} from '$lib/components/ui/dialog/index.js';
	import api from '$lib/api';
	import type { ApiError, DirectoryEntry } from '$lib/types';

	interface Props {
		open: boolean;
		// 对话框标题，如「选择源目录」
		title?: string;
		// 打开时进入的初始路径，为空或不存在时从根目录开始浏览
		initialPath?: string;
		onConfirm: (path: string) => void;
		// 对话框关闭时回调（内部取消或确认后触发），供父组件同步关闭状态
		onclose?: () => void;
	}

	let {
		open = $bindable(false),
		title = '选择目录',
		initialPath = '',
		onConfirm,
		onclose
	}: Props = $props();

	let currentPath = $state('/');
	let parentPath = $state<string | null>(null);
	let entries = $state<DirectoryEntry[]>([]);
	let loading = $state(false);
	let error = $state('');
	// 最后一次失败浏览的路径：重试回到用户请求的目标，而非当前展示路径
	let lastFailedPath = $state('');

	// 请求序号：后发请求胜出；关闭对话框自增，使在途响应过期被丢弃。
	// 非响应式普通变量：请求计数与 effect 无关，避免被 Svelte 追踪引发重复触发或无限循环
	let requestSeq = 0;

	// 打开令牌：记录上次 effect 刷新时 open 的值，仅在开关沿动作一次。
	// 非响应式普通变量，读写均不触发 effect，避免“依赖 open 又写 state”的循环
	let lastEffectOpen = false;

	$effect(() => {
		// 只追踪 open 与 initialPath
		if (open) {
			// 仅首次打开时允许无效初始路径回退根目录；浏览中的错误就地提示
			if (!lastEffectOpen) {
				lastEffectOpen = true;
				loadDirectory(initialPath, { fallbackRootOnError: true });
			}
		} else {
			// 关闭对话框：使在途响应过期并复位状态与令牌
			if (lastEffectOpen) {
				lastEffectOpen = false;
				// 完整复位，下次打开看到干净列表；在途响应因 requestSeq 自增而过期
				requestSeq += 1;
				currentPath = '/';
				parentPath = null;
				entries = [];
				error = '';
				loading = false;
			}
		}
	});

	async function loadDirectory(path?: string, opts: { fallbackRootOnError?: boolean } = {}) {
		// 允许本次请求写状态的最低序号：并发下只有最后发起的请求能胜出
		// loadDirectory 由事件或 $effect 调用；对状态的全部写入用 untrack 包裹，
		// 避免 effect 追踪触发 effect_update_depth_exceeded / 无限请求
		const seq = requestSeq + 1;
		requestSeq = seq;
		untrack(() => {
			lastFailedPath = path ?? '';
			loading = true;
			error = '';
		});
		try {
			const response = await api.browseDirectory(path);
			if (seq !== requestSeq) return;
			untrack(() => {
				currentPath = response.data.path;
				parentPath = response.data.parent;
				entries = response.data.entries;
			});
		} catch (err) {
			if (seq !== requestSeq) return;
			console.error('浏览目录失败：', err);
			// 仅首次打开的无效初始路径回退根目录；用户浏览中的子目录错误就地提示并重试
			if (opts.fallbackRootOnError && path && path !== '/') {
				await loadDirectory('/', { fallbackRootOnError: true });
				return;
			}
			untrack(() => {
				error = (err as ApiError).message;
			});
		} finally {
			loading = false;
		}
	}

	function confirm() {
		onConfirm(currentPath);
		open = false;
	}

	// 打开期间父组件若提前卸载/切换 pickerTarget，本组件可能被销毁，清理对话框状态。
	// onclose 非响应式 prop，写入 untrack 结果，避免 effect 同时依赖 open 与回调
	$effect(() => {
		if (!open && onclose) {
			untrack(() => onclose?.());
		}
	});
</script>

<Dialog bind:open>
	<DialogContent class="flex h-[70vh] max-h-[70vh] flex-col overflow-hidden sm:max-w-lg">
		<DialogHeader class="shrink-0">
			<DialogTitle class="flex items-center gap-2">
				<FolderOpenIcon class="h-5 w-5" aria-hidden="true" />
				{title}
			</DialogTitle>
			<DialogDescription>浏览服务器（或容器）内的目录，仅列出子目录</DialogDescription>
		</DialogHeader>

		<div class="bg-muted/30 flex shrink-0 items-center gap-2 rounded-md border px-3 py-2">
			<Button
				variant="outline"
				size="sm"
				class="h-7 shrink-0 cursor-pointer px-2 text-xs"
				disabled={!parentPath || loading}
				onclick={() => loadDirectory(parentPath ?? undefined)}
				aria-label="返回上一级目录"
			>
				<CornerLeftUpIcon class="h-3.5 w-3.5" aria-hidden="true" />
				上一级
			</Button>
			<p
				class="text-muted-foreground min-w-0 flex-1 truncate font-mono text-xs"
				title={currentPath}
			>
				{currentPath}
			</p>
		</div>

		{#if loading && entries.length === 0}
			<div class="text-muted-foreground/70 flex flex-1 items-center justify-center gap-2">
				<LoaderCircleIcon class="h-5 w-5 animate-spin" aria-hidden="true" />
				正在读取目录...
			</div>
		{:else if error}
			<div
				role="alert"
				class="text-destructive bg-destructive/10 flex flex-col items-center justify-center gap-3 rounded-lg p-6 text-sm"
			>
				<span class="text-center">{error}</span>
				<Button
					variant="outline"
					class="cursor-pointer"
					onclick={() =>
						loadDirectory(lastFailedPath || undefined, {
							fallbackRootOnError: false
						})}
					disabled={loading}>重试</Button
				>
			</div>
		{:else if entries.length === 0}
			<div class="text-muted-foreground/70 flex flex-1 items-center justify-center text-sm">
				此目录下没有子目录
			</div>
		{:else}
			<div class="min-h-0 flex-1 overflow-y-auto" class:opacity-50={loading} aria-busy={loading}>
				<ul class="space-y-0.5 py-1">
					{#each entries as entry (entry.path)}
						<li>
							<button
								type="button"
								class="hover:bg-accent hover:text-accent-foreground focus-visible:ring-ring flex w-full cursor-pointer items-center gap-2 rounded-md px-3 py-2 text-left text-sm focus-visible:outline-none focus-visible:ring-2 disabled:cursor-not-allowed disabled:opacity-60"
								onclick={() => loadDirectory(entry.path)}
								disabled={loading}
							>
								<FolderIcon class="text-muted-foreground h-4 w-4 shrink-0" aria-hidden="true" />
								<span class="min-w-0 flex-1 truncate">{entry.name}</span>
							</button>
						</li>
					{/each}
				</ul>
			</div>
		{/if}

		<DialogFooter class="shrink-0">
			<Button variant="outline" class="cursor-pointer" onclick={() => (open = false)}>取消</Button>
			<Button class="cursor-pointer" onclick={confirm} disabled={loading || !!error}>
				选择此目录
			</Button>
		</DialogFooter>
	</DialogContent>
</Dialog>
