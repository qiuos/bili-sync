<script lang="ts">
	import { onMount } from 'svelte';
	import { Card, CardContent, CardHeader, CardTitle } from '$lib/components/ui/card/index.js';
	import { Progress } from '$lib/components/ui/progress/index.js';
	import { Badge } from '$lib/components/ui/badge/index.js';
	import { Button } from '$lib/components/ui/button/index.js';
	import { setBreadcrumb } from '$lib/stores/breadcrumb';
	import { toast } from 'svelte-sonner';
	import CloudDownloadIcon from '@lucide/svelte/icons/cloud-download';
	import api from '$lib/api';
	import type { DashBoardResponse, SysInfo, ApiError, TaskStatus, LogEntry } from '$lib/types';
	import ChevronRightIcon from '@lucide/svelte/icons/chevron-right';
	import CpuIcon from '@lucide/svelte/icons/cpu';
	import HardDriveIcon from '@lucide/svelte/icons/hard-drive';
	import ListIcon from '@lucide/svelte/icons/list';
	import MemoryStickIcon from '@lucide/svelte/icons/memory-stick';
	import PlayIcon from '@lucide/svelte/icons/play';
	import VideoCover from '$lib/components/video-cover.svelte';
	import { getOverallStatus } from '$lib/video-status';
	import { formatCreatedAt, pushLog } from '$lib/dashboard-data';

	// 紧凑仪表盘：下载任务、三张资源卡、最近入库 5 条、最新日志 10 条（签核草图为绑定方向）
	let dashboardData = $state<DashBoardResponse | null>(null);
	let dashboardError = $state('');
	let sysInfo = $state<SysInfo | null>(null);
	let taskStatus = $state<TaskStatus | null>(null);
	let loading = $state(false);
	let triggering = $state(false);
	// 最新日志：最新在上，最多保留 10 条
	let logs = $state<LogEntry[]>([]);
	let unsubscribers: Array<() => void> = [];

	function formatBytes(bytes: number): string {
		if (bytes === 0) return '0 B';
		const k = 1024;
		const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
		const i = Math.floor(Math.log(bytes) / Math.log(k));
		return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
	}

	function formatCpu(cpu: number): string {
		return `${cpu.toFixed(1)}%`;
	}

	// 入库时间与日志缓冲逻辑在 $lib/dashboard-data.ts；日志时间戳「10:08:32」直接展示
	function formatLogTime(timestamp: string): string {
		return timestamp;
	}

	async function loadDashboard() {
		loading = true;
		dashboardError = '';
		try {
			const response = await api.getDashboard();
			dashboardData = response.data;
		} catch (error) {
			console.error('加载仪表盘数据失败：', error);
			dashboardError = (error as ApiError).message || '加载失败';
		} finally {
			loading = false;
		}
	}

	async function handleTriggerDownload() {
		triggering = true;
		try {
			await api.triggerDownloadTask();
			toast.success('已触发下载任务', {
				description: '任务将立即开始执行'
			});
		} catch (error) {
			console.error('触发下载任务失败：', error);
			toast.error('触发下载任务失败', {
				description: (error as ApiError).message
			});
		} finally {
			triggering = false;
		}
	}

	const diskUsagePercent = $derived(
		sysInfo
			? Math.min(100, ((sysInfo.total_disk - sysInfo.available_disk) / sysInfo.total_disk) * 100)
			: 0
	);

	// 订阅清理由 ws.ts 引用计数完成，这里只保留退订句柄
	onMount(() => {
		setBreadcrumb([{ label: '仪表盘' }]);

		unsubscribers.push(
			api.subscribeToSysInfo((data) => {
				sysInfo = data;
			})
		);
		unsubscribers.push(
			api.subscribeToTasks((data: TaskStatus) => {
				taskStatus = data;
			})
		);
		unsubscribers.push(
			api.subscribeToLogs((data: string) => {
				try {
					logs = pushLog(logs, JSON.parse(data) as LogEntry);
				} catch (error) {
					console.error('解析日志消息失败：', error);
				}
			})
		);
		loadDashboard();
		return () => {
			unsubscribers.forEach((unsub) => unsub());
			unsubscribers = [];
		};
	});
</script>

<svelte:head>
	<title>仪表盘 - Bili Sync</title>
</svelte:head>

<div class="space-y-4">
	<!-- 下载任务卡：状态与下次运行 + 突出的执行按钮；不展示上次开始/结束（PRD R1） -->
	<Card>
		<CardHeader class="flex flex-row items-center justify-between space-y-0 pb-2">
			<CardTitle class="text-sm font-medium">下载任务</CardTitle>
			<Badge variant={taskStatus?.is_running ? 'default' : 'outline'}>
				{taskStatus ? (taskStatus.is_running ? '正在运行' : '未运行') : '...'}
			</Badge>
		</CardHeader>
		<CardContent>
			<div class="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
				<div class="min-w-0 space-y-1">
					<div class="text-muted-foreground text-sm">
						{taskStatus ? (taskStatus.is_running ? '任务正在执行' : '准备就绪') : '加载中...'}
					</div>
					<div class="flex items-center gap-2 text-sm">
						<PlayIcon class="text-muted-foreground h-4 w-4 shrink-0" />
						<span>下次运行</span>
						<span class="font-medium">
							{#if taskStatus?.next_run}
								{new Date(taskStatus.next_run).toLocaleString('zh-CN', {
									month: '2-digit',
									day: '2-digit',
									hour: '2-digit',
									minute: '2-digit',
									hour12: false
								})}
							{:else}
								-
							{/if}
						</span>
					</div>
				</div>
				<Button
					class="w-full cursor-pointer sm:w-auto"
					size="lg"
					onclick={handleTriggerDownload}
					disabled={triggering || (taskStatus?.is_running ?? false)}
				>
					<CloudDownloadIcon class="h-4 w-4" />
					{triggering ? '触发中...' : taskStatus?.is_running ? '任务运行中' : '立即执行下载任务'}
				</Button>
			</div>
		</CardContent>
	</Card>

	<!-- 资源卡：存储 / CPU / 内存；CPU 与内存展示整体和程序占用，无历史图（PRD R2） -->
	<div class="grid gap-4 sm:grid-cols-3">
		<Card>
			<CardHeader class="flex flex-row items-center justify-between space-y-0 pb-2">
				<CardTitle class="text-sm font-medium">存储空间</CardTitle>
				<HardDriveIcon class="text-muted-foreground h-4 w-4" />
			</CardHeader>
			<CardContent>
				{#if sysInfo}
					<div class="space-y-2">
						<div class="text-2xl font-bold tabular-nums">{formatBytes(sysInfo.available_disk)}</div>
						<div class="text-muted-foreground text-xs">
							可用 / 共 {formatBytes(sysInfo.total_disk)}
						</div>
						<Progress value={diskUsagePercent} class="h-2" />
						<div class="text-muted-foreground text-xs">已用 {diskUsagePercent.toFixed(1)}%</div>
					</div>
				{:else}
					<div class="text-muted-foreground text-sm">加载中...</div>
				{/if}
			</CardContent>
		</Card>
		<Card>
			<CardHeader class="flex flex-row items-center justify-between space-y-0 pb-2">
				<CardTitle class="text-sm font-medium">CPU</CardTitle>
				<CpuIcon class="text-muted-foreground h-4 w-4" />
			</CardHeader>
			<CardContent>
				{#if sysInfo}
					<div class="space-y-1 text-sm">
						<div class="flex items-center justify-between">
							<span class="text-muted-foreground">整体</span>
							<span class="text-2xl font-bold tabular-nums">{formatCpu(sysInfo.used_cpu)}</span>
						</div>
						<div class="text-muted-foreground flex items-center justify-between">
							<span>程序</span>
							<span class="tabular-nums">{formatCpu(sysInfo.process_cpu)}</span>
						</div>
					</div>
				{:else}
					<div class="text-muted-foreground text-sm">加载中...</div>
				{/if}
			</CardContent>
		</Card>
		<Card>
			<CardHeader class="flex flex-row items-center justify-between space-y-0 pb-2">
				<CardTitle class="text-sm font-medium">内存</CardTitle>
				<MemoryStickIcon class="text-muted-foreground h-4 w-4" />
			</CardHeader>
			<CardContent>
				{#if sysInfo}
					<div class="space-y-1 text-sm">
						<div class="flex items-center justify-between">
							<span class="text-muted-foreground">整体</span>
							<span class="text-2xl font-bold tabular-nums">
								{formatBytes(sysInfo.used_memory)} / {formatBytes(sysInfo.total_memory)}
							</span>
						</div>
						<div class="text-muted-foreground flex items-center justify-between">
							<span>程序</span>
							<span class="tabular-nums">{formatBytes(sysInfo.process_memory)}</span>
						</div>
					</div>
				{:else}
					<div class="text-muted-foreground text-sm">加载中...</div>
				{/if}
			</CardContent>
		</Card>
	</div>

	<!-- 最近入库与最新日志：桌面并排等高（固定高度、内容内部滚动）、手机堆叠自适应高度 -->
	<div class="grid gap-4 lg:grid-cols-12">
		<!-- 最近入库：最近 5 条，倒序；封面/标题/状态/入库时间（PRD R3） -->
		<div class="lg:col-span-7 lg:h-96 min-w-0">
			<Card class="flex h-full flex-col overflow-hidden">
				<CardHeader class="flex flex-row items-center justify-between space-y-0 pb-2 pt-6 shrink-0">
					<CardTitle class="text-sm font-medium">最近入库</CardTitle>
					<a
						href="/videos"
						class="text-muted-foreground hover:text-foreground inline-flex items-center gap-0.5 rounded-sm text-sm focus-visible:ring-ring focus-visible:outline-none focus-visible:ring-2"
					>
						查看全部
						<ChevronRightIcon class="h-4 w-4" />
					</a>
				</CardHeader>
				<CardContent class="min-h-0 flex-1 overflow-y-auto">
					{#if loading}
						<div class="text-muted-foreground py-6 text-center text-sm">加载中...</div>
					{:else if dashboardError}
						<div
							role="alert"
							class="text-destructive bg-destructive/10 flex flex-col items-center justify-center gap-3 rounded-lg p-6 text-sm"
						>
							<span class="text-center">{dashboardError}</span>
							<Button variant="outline" class="cursor-pointer" onclick={loadDashboard}>重试</Button>
						</div>
					{:else if dashboardData && dashboardData.recent_videos.length > 0}
						<ul class="divide-y">
							{#each dashboardData.recent_videos as video (video.id)}
								{@const overall = getOverallStatus(
									video.download_status,
									video.should_download,
									video.valid
								)}
								<li class="py-2 first:pt-0 last:pb-0">
									<a
										href="/video/{video.id}"
										class="hover:bg-accent/50 focus-visible:ring-ring -my-1 flex items-center gap-3 rounded-md p-2 transition-colors focus-visible:outline-none focus-visible:ring-2"
									>
										<div class="w-20 shrink-0">
											<VideoCover videoId={video.id} alt={video.name} class="rounded-md" />
										</div>
										<div class="flex min-w-0 flex-1 items-center justify-between gap-3">
											<span class="line-clamp-2 min-w-0 text-sm font-medium" title={video.name}>
												{video.name}
											</span>
											<div class="flex shrink-0 flex-col items-end gap-1">
												<Badge variant="secondary" class="px-1.5 py-0.5 text-xs {overall.style}">
													{overall.text}
												</Badge>
												<span class="text-muted-foreground text-xs tabular-nums">
													{formatCreatedAt(video.created_at)}
												</span>
											</div>
										</div>
									</a>
								</li>
							{/each}
						</ul>
					{:else}
						<div class="text-muted-foreground py-6 text-center text-sm">
							暂无入库记录，等待扫描或下载任务执行
						</div>
					{/if}
				</CardContent>
			</Card>
		</div>

		<!-- 最新日志：实时最近 10 条，最新在上（PRD R4） -->
		<div class="lg:col-span-5 lg:h-96 min-w-0">
			<Card class="flex h-full flex-col overflow-hidden">
				<CardHeader class="flex flex-row items-center justify-between space-y-0 pb-2 pt-6 shrink-0">
					<CardTitle class="text-sm font-medium">最新日志</CardTitle>
					<a
						href="/logs"
						class="text-muted-foreground hover:text-foreground inline-flex items-center gap-0.5 rounded-sm text-sm focus-visible:ring-ring focus-visible:outline-none focus-visible:ring-2"
					>
						<ListIcon class="h-4 w-4" />
						查看全部
						<ChevronRightIcon class="h-4 w-4" />
					</a>
				</CardHeader>
				<CardContent class="min-h-0 flex-1 overflow-y-auto">
					{#if logs.length === 0}
						<div class="text-muted-foreground py-4 text-center text-sm">暂无日志记录</div>
					{:else}
						<div class="space-y-0.5">
							{#each logs as log, index (index)}
								<div
									class="flex items-center gap-3 rounded-md px-1 py-0.5 font-mono text-xs {index %
										2 ===
									0
										? 'bg-muted/50'
										: ''}"
								>
									<span class="text-muted-foreground w-16 shrink-0 tabular-nums">
										{formatLogTime(log.timestamp)}
									</span>
									<Badge
										class="w-14 shrink-0 justify-center px-1 py-0 font-semibold {log.level ===
										'ERROR'
											? 'bg-rose-700 text-white'
											: log.level === 'WARN'
												? 'bg-yellow-700 text-white'
												: 'bg-emerald-700 text-white'}"
									>
										{log.level}
									</Badge>
									<span class="min-w-0 flex-1 truncate" title={log.message}>
										{log.message}
									</span>
								</div>
							{/each}
						</div>
					{/if}
				</CardContent>
			</Card>
		</div>
	</div>
</div>
