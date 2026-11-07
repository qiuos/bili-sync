// 仪表盘日志缓冲与入库时间格式回归检查：bun web/src/routes/dashboard-data.check.ts
// 直接 import 生产实现（$lib/dashboard-data），不做镜像复制：
// 1) 最新日志最多 10 条、最新在上（AC3）
// 2) 入库时间「月-日 时:分」按 UTC 解析、本地时区展示（AC2 展示字段）
// 3) 上海时区下 12:00 UTC 显示 20:00（Coordinator 要求的时区钉住验证）
import { strict as assert } from 'node:assert';
import { formatCreatedAt, pushLog } from '../lib/dashboard-data';
import type { LogEntry } from '../lib/types';

const entry = (i: number): LogEntry => ({
	timestamp: `10:0${i}:00`,
	level: 'INFO',
	message: `msg${i}`
});

// 场景1：12 条日志涌入，只保留 10 条且最新在上
let buffer: LogEntry[] = [];
for (let i = 1; i <= 12; i++) {
	buffer = pushLog(buffer, entry(i));
}
assert.equal(buffer.length, 10, '最多 10 条');
assert.equal(buffer[0].message, 'msg12', '最新在第 0 位');
assert.equal(buffer[9].message, 'msg3', '第 10 位是最早保留的一条');

// 场景2：乱序到达依旧最新在上
buffer = [];
buffer = pushLog(buffer, entry(3));
buffer = pushLog(buffer, entry(1));
buffer = pushLog(buffer, entry(2));
assert.equal(buffer[0].message, 'msg2');
assert.equal(buffer[2].message, 'msg3');

// 场景3：SQLite UTC 串按 UTC 解析（UTC 时区钉住：12:00 UTC 即 12:00）
assert.equal(formatCreatedAt('2026-11-07 12:00:00', 'UTC'), '11-07 12:00');

// 场景4：上海时区（UTC+8）下 12:00 UTC 显示 20:00
assert.equal(formatCreatedAt('2026-11-07 12:00:00', 'Asia/Shanghai'), '11-07 20:00');

// 场景5：解析失败回退原字符串，不抛错
assert.equal(formatCreatedAt('not-a-date'), 'not-a-date');

console.log('仪表盘数据回归检查全部通过');
