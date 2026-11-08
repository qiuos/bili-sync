<script lang="ts">
	import { Button } from '$lib/components/ui/button/index.js';
	import { Badge } from '$lib/components/ui/badge/index.js';
	import * as AlertDialog from '$lib/components/ui/alert-dialog/index.js';
	import * as Tooltip from '$lib/components/ui/tooltip/index.js';
	import LoaderCircleIcon from '@lucide/svelte/icons/loader-circle';
	import PlusIcon from '@lucide/svelte/icons/plus';
	import EyeIcon from '@lucide/svelte/icons/eye';
	import SquarePenIcon from '@lucide/svelte/icons/square-pen';
	import Trash2Icon from '@lucide/svelte/icons/trash-2';
	import PlayIcon from '@lucide/svelte/icons/play';
	import CopyIcon from '@lucide/svelte/icons/copy';
	import TvIcon from '@lucide/svelte/icons/tv';
	import FilmIcon from '@lucide/svelte/icons/film';
	import FolderInputIcon from '@lucide/svelte/icons/folder-input';
	import FolderOutputIcon from '@lucide/svelte/icons/folder-output';
	import FolderSyncIcon from '@lucide/svelte/icons/folder-sync';
	import RuleDialog from '$lib/components/organization/rule-dialog.svelte';
	import PreviewDialog from '$lib/components/organization/preview-dialog.svelte';
	import api from '$lib/api';
	import type { ApiError, OrganizationExecuteResponse, OrganizationRule } from '$lib/types';
	import { onMount } from 'svelte';
	import { setBreadcrumb } from '$lib/stores/breadcrumb';
	import { toast } from 'svelte-sonner';

	let rules = $state<OrganizationRule[] | null>(null);
	let loading = $state(false);
	let loadError = $state('');

	let ruleDialogOpen = $state(false);
	let editingRule = $state<OrganizationRule | null>(null);

	let previewRule = $state<OrganizationRule | null>(null);
	let previewOpen = $state(false);

	// 复制：以预填字段打开新建表单（id=0）。DraftRule 仅预填用，保存走创建接口，保存前不创建不执行
	let draftRule = $state<OrganizationRule | null>(null);

	function openCopy(rule: OrganizationRule) {
		draftRule = { ...rule, id: 0, name: `${rule.name} 副本` };
		editingRule = null;
		ruleDialogOpen = false;
		setTimeout(() => (ruleDialogOpen = true));
	}

	// 列表直接运行：电视剧直接执行；电影从预览取全部候选视频 ID 再执行。
	// 无候选、失败等结果经 toast 提示；运行中禁止重复触发（runningRuleId 锁）。
	let runningRuleId = $state<number | null>(null);

	function showRunResult(rule: OrganizationRule, result: OrganizationExecuteResponse) {
		const description = `新建 ${result.created}，刷新 ${result.refreshed}，跳过 ${result.skipped}，失败 ${result.failed}`;
		const details = [
			...result.warnings,
			...result.items.flatMap((item) => (item.message ? [item.message] : []))
		];
		const uniqueDetails = [...new Set(details)];
		if (result.failed || details.length) {
			toast.warning(`「${rule.name}」整理完成，有待处理事项`, {
				description: `${description}${uniqueDetails.length ? `；${uniqueDetails.slice(0, 3).join('；')}${uniqueDetails.length > 3 ? '；更多信息请查看整理预览' : ''}` : ''}`
			});
		} else {
			toast.success(`「${rule.name}」整理完成`, { description });
		}
	}

	async function runRule(rule: OrganizationRule) {
		if (runningRuleId !== null) return;
		runningRuleId = rule.id;
		try {
			if (rule.rule_type !== 'movie') {
				const response = await api.organization.executeRule(rule.id);
				showRunResult(rule, response.data);
				return;
			}
			const plan = await api.organization.previewRule(rule.id);
			const videoIds = [
				...new Set(
					plan.data.items
						.filter((item) => item.action === 'create' || item.action === 'refresh')
						.map((item) => item.video_id)
				)
			];
			if (videoIds.length === 0) {
				const details = [
					...plan.data.warnings,
					...plan.data.items.flatMap((item) => (item.message ? [item.message] : []))
				];
				toast.info(`「${rule.name}」没有可执行的候选视频`, {
					description:
						[...new Set(details)].slice(0, 3).join('；') ||
						'源目录内没有已下载的电影，或视频均未下载完成'
				});
				return;
			}
			const response = await api.organization.executeRule(rule.id, videoIds);
			showRunResult(rule, response.data);
		} catch (error) {
			console.error('执行整理失败：', error);
			toast.error('执行整理失败', { description: (error as ApiError).message });
		} finally {
			runningRuleId = null;
		}
	}

	let deletingRule = $state<OrganizationRule | null>(null);
	let deleteDialogOpen = $state(false);
	let deleting = $state(false);

	async function loadRules() {
		loading = true;
		loadError = '';
		try {
			const result = await api.organization.listRules();
			rules = result.data.rules;
		} catch (error) {
			console.error('加载整理规则失败：', error);
			loadError = (error as ApiError).message;
			toast.error('加载整理规则失败', { description: (error as ApiError).message });
		} finally {
			loading = false;
		}
	}

	function openCreate() {
		editingRule = null;
		draftRule = null;
		ruleDialogOpen = true;
	}

	function openEdit(rule: OrganizationRule) {
		editingRule = rule;
		ruleDialogOpen = true;
	}

	function openPreview(rule: OrganizationRule) {
		previewRule = rule;
		previewOpen = true;
	}

	function confirmDelete(rule: OrganizationRule) {
		deletingRule = rule;
		deleteDialogOpen = true;
	}

	async function handleDelete() {
		if (!deletingRule) return;
		deleting = true;
		try {
			await api.organization.deleteRule(deletingRule.id);
			toast.success('规则已删除', {
				description: `「${deletingRule.name}」已删除，已生成的文件保留在磁盘上`
			});
			deleteDialogOpen = false;
			await loadRules();
		} catch (error) {
			console.error('删除整理规则失败：', error);
			toast.error('删除整理规则失败', { description: (error as ApiError).message });
		} finally {
			deleting = false;
		}
	}

	onMount(() => {
		setBreadcrumb([{ label: '媒体整理' }]);
		loadRules();
	});
</script>

<svelte:head>
	<title>媒体整理 - Bili Sync</title>
</svelte:head>

<div class="mb-4 flex items-center justify-between">
	<div class="text-sm font-medium">
		{#if rules}
			共 {rules.length} 条规则
		{/if}
	</div>
	<Button size="sm" class="cursor-pointer" onclick={openCreate} disabled={loading}>
		<PlusIcon class="mr-1.5 h-4 w-4" aria-hidden="true" />
		新建规则
	</Button>
</div>

{#if loading && !rules}
	<div class="text-muted-foreground/70 flex items-center justify-center py-16 text-sm">
		<LoaderCircleIcon class="mr-2 h-4 w-4 animate-spin" aria-hidden="true" />
		加载中...
	</div>
{:else if loadError && !rules}
	<div class="flex items-center justify-center py-16">
		<div class="space-y-3 text-center">
			<p class="text-destructive text-sm">{loadError}</p>
			<Button variant="outline" class="cursor-pointer" onclick={loadRules}>重试</Button>
		</div>
	</div>
{:else if rules && rules.length === 0}
	<div class="flex items-center justify-center py-16">
		<div class="space-y-3 text-center">
			<FolderSyncIcon class="text-muted-foreground/40 mx-auto h-10 w-10" aria-hidden="true" />
			<p class="text-muted-foreground text-sm">还没有整理规则</p>
			<p class="text-muted-foreground/70 text-xs">
				创建规则，将下载的视频整理成 Emby 可用的硬链接媒体库
			</p>
			<Button size="sm" class="cursor-pointer" onclick={openCreate}>
				<PlusIcon class="mr-1.5 h-4 w-4" aria-hidden="true" />
				新建规则
			</Button>
		</div>
	</div>
{:else if rules}
	<div class="mb-8 grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3">
		{#each rules as rule (rule.id)}
			<div class="bg-card text-card-foreground flex flex-col gap-3 rounded-xl border p-5 shadow-sm">
				<div class="flex items-center gap-2">
					<h3 class="min-w-0 flex-1 truncate font-semibold">{rule.name}</h3>
					{#if rule.rule_type === 'tv'}
						<Badge variant="outline" class="shrink-0 gap-1">
							<TvIcon class="h-3 w-3" aria-hidden="true" />
							电视剧
						</Badge>
					{:else}
						<Badge variant="outline" class="shrink-0 gap-1">
							<FilmIcon class="h-3 w-3" aria-hidden="true" />
							电影
						</Badge>
					{/if}
				</div>

				<div class="min-w-0 flex-1 space-y-2 text-sm">
					<div class="flex items-start gap-2">
						<FolderInputIcon
							class="text-muted-foreground mt-0.5 h-4 w-4 shrink-0"
							aria-hidden="true"
						/>
						<div class="min-w-0">
							<div class="text-muted-foreground text-xs">源目录</div>
							<p class="truncate font-mono text-xs" title={rule.source_path}>
								{rule.source_path}
							</p>
						</div>
					</div>
					<div class="flex items-start gap-2">
						<FolderOutputIcon
							class="text-muted-foreground mt-0.5 h-4 w-4 shrink-0"
							aria-hidden="true"
						/>
						<div class="min-w-0">
							<div class="text-muted-foreground text-xs">输出目录</div>
							<p class="truncate font-mono text-xs" title={rule.output_path}>
								{rule.output_path}
							</p>
						</div>
					</div>
				</div>

				<div class="flex items-center gap-2 border-t pt-3">
					<Button
						size="sm"
						variant="outline"
						class="h-8 cursor-pointer text-xs font-medium"
						onclick={() => openPreview(rule)}
					>
						<EyeIcon class="mr-1.5 h-3.5 w-3.5" aria-hidden="true" />
						预览整理
					</Button>
					<Button
						size="sm"
						variant="outline"
						class="h-8 cursor-pointer text-xs font-medium"
						disabled={runningRuleId !== null}
						onclick={() => runRule(rule)}
					>
						{#if runningRuleId === rule.id}
							<LoaderCircleIcon class="mr-1.5 h-3.5 w-3.5 animate-spin" aria-hidden="true" />
							运行中...
						{:else}
							<PlayIcon class="mr-1.5 h-3.5 w-3.5" aria-hidden="true" />
							运行
						{/if}
					</Button>
					<Tooltip.Root>
						<Tooltip.Trigger
							class="hover:bg-accent hover:text-accent-foreground inline-flex h-8 w-8 cursor-pointer items-center justify-center rounded-md border"
							aria-label="编辑规则"
							onclick={() => openEdit(rule)}
						>
							<SquarePenIcon class="h-3.5 w-3.5" aria-hidden="true" />
						</Tooltip.Trigger>
						<Tooltip.Content>编辑</Tooltip.Content>
					</Tooltip.Root>
					<Tooltip.Root>
						<Tooltip.Trigger
							class="hover:bg-accent hover:text-accent-foreground inline-flex h-8 w-8 cursor-pointer items-center justify-center rounded-md border"
							aria-label="复制规则"
							onclick={() => openCopy(rule)}
						>
							<CopyIcon class="h-3.5 w-3.5" aria-hidden="true" />
						</Tooltip.Trigger>
						<Tooltip.Content>复制</Tooltip.Content>
					</Tooltip.Root>
					<Tooltip.Root>
						<Tooltip.Trigger
							class="hover:bg-accent hover:text-destructive inline-flex h-8 w-8 cursor-pointer items-center justify-center rounded-md border"
							aria-label="删除规则"
							onclick={() => confirmDelete(rule)}
						>
							<Trash2Icon class="h-3.5 w-3.5" aria-hidden="true" />
						</Tooltip.Trigger>
						<Tooltip.Content>删除</Tooltip.Content>
					</Tooltip.Root>
				</div>
			</div>
		{/each}
	</div>
{/if}

<RuleDialog bind:open={ruleDialogOpen} rule={editingRule ?? draftRule} onSuccess={loadRules} />

<PreviewDialog bind:open={previewOpen} rule={previewRule} onFinished={null} />

<AlertDialog.Root bind:open={deleteDialogOpen}>
	<AlertDialog.Content>
		<AlertDialog.Header>
			<AlertDialog.Title>删除整理规则</AlertDialog.Title>
			<AlertDialog.Description>
				确定要删除规则「<strong>{deletingRule?.name}</strong>」吗？ 该规则的整理记录将被<span
					class="text-destructive font-medium">永久删除且无法恢复</span
				>，电视剧已分配的集号随之丢失，之后重新整理会按新规则重新编号；<span
					class="text-destructive font-medium">已生成的视频链接和元数据文件仍保留在磁盘上</span
				>。
			</AlertDialog.Description>
		</AlertDialog.Header>
		<AlertDialog.Footer>
			<AlertDialog.Cancel disabled={deleting}>取消</AlertDialog.Cancel>
			<AlertDialog.Action
				onclick={handleDelete}
				disabled={deleting}
				class="bg-destructive hover:bg-destructive/90"
			>
				{#if deleting}
					<LoaderCircleIcon class="mr-2 h-4 w-4 animate-spin" aria-hidden="true" />
					删除中...
				{:else}
					确认删除
				{/if}
			</AlertDialog.Action>
		</AlertDialog.Footer>
	</AlertDialog.Content>
</AlertDialog.Root>
