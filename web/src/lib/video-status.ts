// 视频下载状态展示：文本与配色唯一来源，video-card 与仪表盘共用，避免标签列表分叉。
// 状态语义与 status-editor.svelte 保持一致：0 未开始，1..3 失败N次，4 重试耗尽仍显示失败N次，7 已完成

/** 失效 / 跳过 / 完成 / 失败 / 等待 五种整体状态的语气分类，便于组件选择图标 */
export type VideoTone = 'invalid' | 'skipped' | 'done' | 'failed' | 'waiting';

export interface VideoOverallStatus {
	text: string;
	/** 徽标用的浅色背景深色字组合 */
	style: string;
	/** 封面角标用的饱和背景浅色字组合 */
	chipClass: string;
	tone: VideoTone;
}

export function getStatusText(status: number): string {
	if (status === 7) {
		return '已完成';
	} else if (status === 0) {
		return '未开始';
	} else {
		return `失败${status}次`;
	}
}

export function getOverallStatus(
	downloadStatus: number[],
	shouldDownload: boolean,
	valid: boolean
): VideoOverallStatus {
	if (!valid) {
		// 视频属性表明已失效，或由于各种条件判断（充电视频等）判定为无效的情况
		return {
			text: '失效',
			style: 'bg-gray-100 text-gray-700',
			chipClass: 'bg-gray-700 text-gray-50',
			tone: 'invalid'
		};
	}
	if (!shouldDownload) {
		// 被过滤规则排除，显示为“跳过”
		return {
			text: '跳过',
			style: 'bg-gray-100 text-gray-700',
			chipClass: 'bg-gray-700 text-gray-50',
			tone: 'skipped'
		};
	}
	const completed = downloadStatus.filter((status) => status === 7).length;
	const total = downloadStatus.length;
	const failed = downloadStatus.filter((status) => status !== 7 && status !== 0).length;

	if (completed === total) {
		// 全部完成，显示为“完成”
		return {
			text: '完成',
			style: 'bg-emerald-700 text-emerald-100',
			chipClass: 'bg-emerald-700 text-white',
			tone: 'done'
		};
	} else if (failed > 0) {
		// 出现了失败，显示为“失败”
		return {
			text: '失败',
			style: 'bg-rose-700 text-rose-100',
			chipClass: 'bg-rose-700 text-white',
			tone: 'failed'
		};
	} else {
		// 还未开始，显示为“等待”
		return {
			text: '等待',
			style: 'bg-yellow-700 text-yellow-100',
			chipClass: 'bg-yellow-800 text-white',
			tone: 'waiting'
		};
	}
}

export function getSegmentColor(status: number): string {
	if (status === 7) {
		return 'bg-emerald-500';
	} else if (status === 0) {
		return 'bg-yellow-500';
	} else {
		return 'bg-rose-500';
	}
}
