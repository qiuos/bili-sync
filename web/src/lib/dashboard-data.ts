import type { LogEntry } from './types';

/**
 * 日志/入库时间与日志缓冲的工具函数；+page.svelte 的仪表盘直接调用，
 * dashboard-data.check.ts 也直接 import 这里的实现做回归（避免镜像复制实现）
 */

/** 日志缓冲：新日志插到最前，最多保留 10 条（PRD R4） */
export function pushLog(buffer: LogEntry[], entry: LogEntry): LogEntry[] {
	return [entry, ...buffer.slice(0, 9)];
}

/**
 * 入库时间格式化：SQLite 存 UTC 无时区串（"YYYY-MM-DD HH:MM:SS"），显式按 UTC 解析，
 * 用 `timeZone` 指定的时区展示为「月-日 时:分」（默认浏览器本地时区），
 * 如上海时区下 2026-11-07 12:00 UTC 显示 11-07 20:00
 */
export function formatCreatedAt(value: string, timeZone?: string): string {
	const date = new Date(value.replace(' ', 'T').concat('Z'));
	if (Number.isNaN(date.getTime())) return value;
	return new Intl.DateTimeFormat('zh-CN', {
		timeZone,
		month: '2-digit',
		day: '2-digit',
		hour: '2-digit',
		minute: '2-digit',
		hour12: false
	})
		.format(date)
		.replaceAll('/', '-');
}
