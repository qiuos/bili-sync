<script lang="ts">
	import { Button } from '$lib/components/ui/button/index.js';
	import { Input } from '$lib/components/ui/input/index.js';
	import { Label } from '$lib/components/ui/label/index.js';
	import * as Select from '$lib/components/ui/select/index.js';
	import * as AlertDialog from '$lib/components/ui/alert-dialog/index.js';
	import { toast } from 'svelte-sonner';
	import {
		Sheet,
		SheetContent,
		SheetDescription,
		SheetFooter,
		SheetHeader,
		SheetTitle
	} from '$lib/components/ui/sheet/index.js';
	import api from '$lib/api';
	import FolderOpenIcon from '@lucide/svelte/icons/folder-open';
	import DirectoryPicker from '$lib/components/organization/directory-picker.svelte';
	import type {
		ApiError,
		OrganizationAggregationMode,
		OrganizationRule,
		OrganizationRuleRequest,
		OrganizationRuleType
	} from '$lib/types';

	interface Props {
		open: boolean;
		rule: OrganizationRule | null;
		onSuccess: (() => void) | null;
	}

	let { open = $bindable(false), rule = null, onSuccess = null }: Props = $props();

	const RULE_TYPE_OPTIONS: { value: OrganizationRuleType; label: string }[] = [
		{ value: 'tv', label: '电视剧' },
		{ value: 'movie', label: '电影' }
	];

	let name = $state('');
	let ruleType = $state<OrganizationRuleType>('tv');
	let aggregationMode = $state<OrganizationAggregationMode>('source');
	let switchDialogOpen = $state(false);
	let sourcePath = $state('');
	let outputPath = $state('');
	let loading = $state(false);

	// 目录浏览选择：'source' | 'output' 标记当前为哪个输入框选目录，null 表示关闭
	let pickerTarget = $state<'source' | 'output' | null>(null);

	$effect(() => {
		if (open) {
			name = rule?.name ?? '';
			ruleType = rule?.rule_type ?? 'tv';
			aggregationMode = rule?.pending_aggregation_mode ?? rule?.aggregation_mode ?? 'source';
			sourcePath = rule?.source_path ?? '';
			outputPath = rule?.output_path ?? '';
		}
	});

	function handleSubmit() {
		if (!name.trim() || !sourcePath.trim() || !outputPath.trim()) return;
		if (
			rule &&
			rule.id > 0 &&
			ruleType === 'tv' &&
			(rule.pending_aggregation_mode || aggregationMode !== rule.aggregation_mode)
		) {
			if (
				!rule.pending_aggregation_mode &&
				(name.trim() !== rule.name ||
					sourcePath.trim() !== rule.source_path ||
					outputPath.trim() !== rule.output_path)
			) {
				toast.error('切换聚合方式时不能同时修改其他配置，请分开保存');
				return;
			}
			switchDialogOpen = true;
			return;
		}
		void saveRule();
	}

	async function saveRule() {
		loading = true;
		try {
			const request: OrganizationRuleRequest = {
				name: name.trim(),
				rule_type: ruleType,
				aggregation_mode:
					ruleType === 'tv' ? (rule?.id ? rule.aggregation_mode : aggregationMode) : 'source',
				source_path: sourcePath.trim(),
				output_path: outputPath.trim()
			};
			// id>0 为编辑已有规则；复制草稿（draftRule）带预填字段但 id=0，走新建接口，不触碰原规则
			if (rule && rule.id > 0) {
				if (!rule.pending_aggregation_mode) await api.organization.updateRule(rule.id, request);
				if (
					ruleType === 'tv' &&
					(rule.pending_aggregation_mode || aggregationMode !== rule.aggregation_mode)
				) {
					const result = (
						await api.organization.switchAggregation(
							rule.id,
							aggregationMode,
							Boolean(rule.pending_aggregation_mode)
						)
					).data;
					const issues = result.execute.items.filter(
						(item) =>
							item.action === 'conflict' ||
							item.action === 'missing' ||
							item.action === 'failed' ||
							item.action === 'exists'
					);
					if (
						issues.length ||
						result.execute.failed ||
						result.retained_paths.length ||
						result.execute.warnings.length
					) {
						toast.warning(
							issues.length || result.execute.failed
								? '切换未完成，请处理后重试'
								: '切换完成，有保留文件或提示',
							{
								description: [
									...issues.map((item) => item.message),
									...result.execute.warnings,
									...result.retained_paths
								]
									.filter(Boolean)
									.join('；')
							}
						);
					} else {
						toast.success('聚合方式已切换并重新整理');
					}
				} else {
					toast.success('规则已更新', { description: `「${request.name}」已保存` });
				}
			} else {
				await api.organization.createRule(request);
				toast.success('规则已创建', { description: `「${request.name}」已保存` });
			}
			open = false;
		} catch (error) {
			console.error('保存整理规则失败：', error);
			toast.error(rule && rule.id > 0 ? '更新规则失败' : '创建规则失败', {
				description: (error as ApiError).message
			});
		} finally {
			loading = false;
			onSuccess?.();
		}
	}
</script>

<Sheet bind:open>
	<SheetContent side="right" class="flex w-full flex-col sm:max-w-md">
		<SheetHeader class="px-6 pb-2">
			<SheetTitle class="text-lg">
				{rule && rule.id > 0 ? '编辑整理规则' : '新建整理规则'}
			</SheetTitle>
			<SheetDescription class="text-muted-foreground text-sm">
				{rule && rule.id > 0
					? `修改规则「${rule.name}」的配置`
					: '配置一条从源目录到输出媒体库的整理规则'}
			</SheetDescription>
		</SheetHeader>

		<div class="flex-1 overflow-y-auto px-6">
			<div class="space-y-5 py-4">
				<div class="space-y-2">
					<Label for="rule-name" class="text-sm font-medium">
						规则名称 <span class="text-destructive">*</span>
					</Label>
					<Input
						id="rule-name"
						type="text"
						placeholder="例如：番剧整理"
						bind:value={name}
						maxlength={100}
						disabled={loading || Boolean(rule?.pending_aggregation_mode)}
					/>
				</div>

				<div class="space-y-2">
					<Label class="text-sm font-medium">整理类型</Label>
					<Select.Root
						type="single"
						value={ruleType}
						onValueChange={(v) => (ruleType = v as OrganizationRuleType)}
						disabled={loading || Boolean(rule?.pending_aggregation_mode)}
					>
						<Select.Trigger class="w-full">
							{RULE_TYPE_OPTIONS.find((option) => option.value === ruleType)?.label ?? ruleType}
						</Select.Trigger>
						<Select.Content>
							{#each RULE_TYPE_OPTIONS as option (option.value)}
								<Select.Item value={option.value} label={option.label} />
							{/each}
						</Select.Content>
					</Select.Root>
					<p class="text-muted-foreground text-xs">
						{ruleType === 'tv'
							? '电视剧：每个分 P 对应一集'
							: '电影：不按 UP 主聚合，需要手动选择视频，多P视为同一电影的多个版本'}
					</p>
				</div>

				{#if ruleType === 'tv'}
					<div class="space-y-2">
						<Label for="aggregation-mode" class="text-sm font-medium">聚合方式</Label>
						<Select.Root
							type="single"
							value={aggregationMode}
							onValueChange={(v) => (aggregationMode = v as OrganizationAggregationMode)}
							disabled={loading || Boolean(rule?.pending_aggregation_mode)}
						>
							<Select.Trigger id="aggregation-mode" class="w-full">
								{aggregationMode === 'source' ? '按源目录聚合' : '按视频聚合'}
							</Select.Trigger>
							<Select.Content>
								<Select.Item value="source" label="按源目录聚合" />
								<Select.Item value="per_video" label="按视频聚合" />
							</Select.Content>
						</Select.Root>
						<p class="text-muted-foreground text-xs">
							{aggregationMode === 'source'
								? '源目录及子目录内所有视频聚合为一部剧，源目录名作为剧名。'
								: '每个视频作为一部剧，视频标题作为剧名；单 P 为一集。'}
						</p>
						{#if rule?.pending_aggregation_mode}
							<p class="text-destructive text-xs" role="alert">上次切换未完成，保存后确认重试。</p>
						{/if}
					</div>
				{/if}

				<div class="space-y-2">
					<Label for="rule-source" class="text-sm font-medium">
						源目录 <span class="text-destructive">*</span>
					</Label>
					<div class="flex items-start gap-2">
						<Input
							id="rule-source"
							type="text"
							placeholder="例如：/media/downloads/bilibili"
							bind:value={sourcePath}
							disabled={loading || Boolean(rule?.pending_aggregation_mode)}
							class="flex-1 font-mono text-sm"
						/>
						<Button
							type="button"
							variant="outline"
							class="shrink-0 cursor-pointer px-3"
							disabled={loading || Boolean(rule?.pending_aggregation_mode)}
							aria-label="浏览选择源目录"
							onclick={() => (pickerTarget = 'source')}
						>
							<FolderOpenIcon class="h-4 w-4" aria-hidden="true" />
							浏览
						</Button>
					</div>
					<p class="text-muted-foreground text-xs">待整理视频所在的下载目录，须为绝对路径。</p>
				</div>

				<div class="space-y-2">
					<Label for="rule-output" class="text-sm font-medium">
						输出目录 <span class="text-destructive">*</span>
					</Label>
					<div class="flex items-start gap-2">
						<Input
							id="rule-output"
							type="text"
							placeholder="例如：/media/emby/电视剧"
							bind:value={outputPath}
							disabled={loading || Boolean(rule?.pending_aggregation_mode)}
							class="flex-1 font-mono text-sm"
						/>
						<Button
							type="button"
							variant="outline"
							class="shrink-0 cursor-pointer px-3"
							disabled={loading || Boolean(rule?.pending_aggregation_mode)}
							aria-label="浏览选择输出目录"
							onclick={() => (pickerTarget = 'output')}
						>
							<FolderOpenIcon class="h-4 w-4" aria-hidden="true" />
							浏览
						</Button>
					</div>
					<p class="text-muted-foreground text-xs">
						硬链接媒体库目录，须为绝对路径且与源文件位于同一文件系统；不能与任何源目录或其他输出目录相同或互相嵌套。
					</p>
				</div>
			</div>
		</div>

		<SheetFooter class="bg-background flex gap-2 border-t px-6 pt-4">
			<Button
				variant="outline"
				onclick={() => (open = false)}
				disabled={loading}
				class="flex-1 cursor-pointer"
			>
				取消
			</Button>
			<Button
				onclick={handleSubmit}
				disabled={loading || !name.trim() || !sourcePath.trim() || !outputPath.trim()}
				class="flex-1 cursor-pointer"
			>
				{loading ? '保存中...' : '保存'}
			</Button>
		</SheetFooter>
	</SheetContent>
</Sheet>

<AlertDialog.Root bind:open={switchDialogOpen}>
	<AlertDialog.Content>
		<AlertDialog.Header>
			<AlertDialog.Title>确认切换聚合方式？</AlertDialog.Title>
			<AlertDialog.Description>
				将删除可确认归属的旧整理硬链接和整理记录，然后按新方式重新整理。原下载文件保留；无法确认归属的文件不会删除或覆盖，可能导致重整冲突。操作中断后需要重试。
			</AlertDialog.Description>
		</AlertDialog.Header>
		<AlertDialog.Footer>
			<AlertDialog.Cancel disabled={loading}>取消</AlertDialog.Cancel>
			<AlertDialog.Action disabled={loading} onclick={() => void saveRule()}
				>确认切换</AlertDialog.Action
			>
		</AlertDialog.Footer>
	</AlertDialog.Content>
</AlertDialog.Root>

<!-- 服务端目录浏览选择：确认后回填输入框，仍可手动修改；原有路径校验保持不变 -->
<DirectoryPicker
	open={pickerTarget !== null}
	title={pickerTarget === 'output' ? '选择输出目录' : '选择源目录'}
	initialPath={pickerTarget === 'output'
		? (rule?.output_path ?? outputPath)
		: (rule?.source_path ?? sourcePath)}
	onConfirm={(path) => {
		if (pickerTarget === 'output') {
			outputPath = path;
		} else if (pickerTarget === 'source') {
			sourcePath = path;
		}
		pickerTarget = null;
	}}
	onclose={() => {
		if (pickerTarget !== null) {
			pickerTarget = null;
		}
	}}
/>
