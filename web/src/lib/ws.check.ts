// WebSocket 订阅回归检查：bun web/src/lib/ws.check.ts
// 用 mock WebSocket 驱动真实 WebSocketManager 行为（AC3 + promise/意图竞态修复）：
// 1) 正常订阅/分发/引用计数（既有行为回归）
// 2) 连接失败必须 settle：open 前关闭 → connect reject，全程无 unhandledRejection
// 3) 未连接时 subscribe/unsubscribe 只改本地意图，不排队旧控制帧；onopen 统一同步「意图 ∩ 有回调」
// 4) 连接中退订（open 前 unsub）→ settle 后无 subscribe 帧、无 unsubscribe 帧
// 5) 退订后立即重订阅 → 意图为一份，重开后恰一条 subscribe 帧、零条 unsubscribe 帧
// 6) disconnect（退出认证）后不自动重连：跨过首次重连延迟（1000ms）后无新 socket 构造
import { strict as assert } from 'node:assert';

// ws.ts 依赖 svelte-sonner（Svelte runes 编译产物），bun 直跑无法解析；用插件替换为空实现
// eslint-disable-next-line @typescript-eslint/no-explicit-any -- 直跑脚本依赖 bun 全局，无 @types/bun
const bun = (globalThis as any).Bun;
bun.plugin({
	name: 'stub-svelte-sonner',
	setup(build: { module: (name: string, fn: () => unknown) => void }) {
		build.module('svelte-sonner', () => ({
			exports: {
				toast: Object.assign(() => {}, { success() {}, error() {}, info() {}, warning() {} }),
				Toaster: class {}
			},
			loader: 'object'
		}));
	}
});

// ---- mock WebSocket：记录发送内容、状态与构造次数，供测试断言 ----
const sentFrames: string[] = [];
let activeSocket: MockSocket | null = null;
let constructedSockets = 0;
// 模拟前 N 次构造同步抛错（如 btoa/构造异常），用于连接失败恢复回归
let failNextConstructions = 0;

class MockSocket {
	onopen: (() => void) | null = null;
	onmessage: ((ev: { data: string }) => void) | null = null;
	onclose: (() => void) | null = null;
	onerror: ((ev: unknown) => void) | null = null;
	readyState = 0;

	constructor(public url: string) {
		constructedSockets++;
		if (failNextConstructions > 0) {
			failNextConstructions--;
			throw new Error('constructor boom');
		}
		// eslint-disable-next-line @typescript-eslint/no-this-alias -- 测试钩子：跟踪最新 socket
		activeSocket = this;
	}

	send(data: string) {
		if (this.readyState !== 1) throw new Error('socket not open');
		sentFrames.push(data);
	}

	close() {
		this.readyState = 3;
		this.onclose?.();
	}
}

// 未处理 rejection 监视：修复后应为 0
let unhandledCount = 0;
process.on('unhandledRejection', () => {
	unhandledCount++;
});

function serverOpen() {
	activeSocket!.readyState = 1;
	activeSocket!.onopen?.();
}

function serverCloseWithoutOpen() {
	activeSocket!.readyState = 3;
	activeSocket!.onclose?.();
}

function closeActiveSocket() {
	activeSocket!.close();
}

function serverPush(payload: string) {
	activeSocket!.onmessage?.({ data: payload });
}

const flush = async (n = 3) => {
	for (let i = 0; i < n; i++) await new Promise((r) => setTimeout(r, 0));
};

// ---- stub 浏览器环境 ----
// eslint-disable-next-line @typescript-eslint/no-explicit-any -- stub 环境，绕开 DOM 类型
const g = globalThis as any;
g.window = { location: { protocol: 'http:', host: 'localhost:5173' } };
g.localStorage = {
	store: {} as Record<string, string>,
	getItem(k: string) {
		return this.store[k] ?? null;
	},
	setItem(k: string, v: string) {
		this.store[k] = v;
	},
	removeItem(k: string) {
		delete this.store[k];
	}
};
g.WebSocket = MockSocket;

const { WebSocketManager } = (await import('./ws')) as typeof import('./ws');
const manager = WebSocketManager.getInstance();

async function connectAndOpen() {
	const connecting = manager.connect();
	await flush();
	assert.ok(activeSocket, 'mock socket 已创建');
	serverOpen();
	await flush();
	await connecting;
}

function kindsOf(kind: 'subscribe' | 'unsubscribe') {
	return sentFrames
		.map((f) => JSON.parse(f))
		.filter((f) => f[kind])
		.map((f) => f[kind])
		.sort();
}

const rawLog = JSON.stringify({ timestamp: '10:00:00', level: 'INFO', message: 'hello' });

// ---- 场景 1：订阅→open 同步一帧、推送分发、引用计数 ----
const logsLine: string[] = [];
const _disposeLogs = manager.subscribeToLogs((data: string) => logsLine.push(data));
await flush(); // 订阅 fire-and-forget，等 MockSocket 构造
assert.ok(activeSocket, 'subscribe 触发连接，mock socket 已创建');
await connectAndOpen();
assert.deepEqual(kindsOf('subscribe'), ['logs'], 'open 后同步恰好一条订阅帧');
sentFrames.length = 0;

let other = 0;
const disposeOther = manager.subscribeToLogs(() => {
	other += 1;
});
serverPush(JSON.stringify({ logs: rawLog }));
assert.deepEqual(logsLine, [rawLog], '日志分发到回调');
assert.equal(other, 1, '推送同时到达两个订阅者');
disposeOther();
await flush();
assert.deepEqual(kindsOf('unsubscribe'), [], 'logs 仍有订阅者，无退订帧');
sentFrames.length = 0;

const _disposeTasks = manager.subscribeToTasks(() => {});
const _disposeSys = manager.subscribeToSysInfo(() => {});
await flush();
assert.deepEqual(kindsOf('subscribe'), ['sysInfo', 'tasks'], '新事件各发一帧订阅');

// ---- 场景 2：断线（close）→ 意图保留 → 新连接 open 补发全部活性订阅 ----
sentFrames.length = 0;
closeActiveSocket(); // onclose → scheduleReconnect（1s 定时器）
await connectAndOpen(); // 立即重连，模拟定时器到点后的新连接
assert.deepEqual(kindsOf('subscribe'), ['logs', 'sysInfo', 'tasks'], '重连补发全部活性订阅');
sentFrames.length = 0;

// ---- 场景 3：连接失败必须 settle，且 open 前关闭无任何控制帧 ----
manager.disconnect(); // 清场景：断开+清意图（回调场景另行重建）
const failLine: string[] = [];
const disposeFail = manager.subscribeToLogs((d) => failLine.push(d));
await flush();
const failing = manager.connect();
await flush();
serverCloseWithoutOpen(); // 未 open 即关闭 → onclose 落锤 reject
await assert.rejects(failing, /closed before open/, 'open 前关闭 → connect reject settle');
assert.deepEqual(
	kindsOf('subscribe').concat(kindsOf('unsubscribe')),
	[],
	'连接失败期间无任何控制帧（不排队旧帧）'
);
// 意图已登记 → 新连接 open 后活性订阅同步恰一帧
await connectAndOpen();
assert.deepEqual(kindsOf('subscribe'), ['logs'], 'open 后同步活性订阅（无重复 subscribe 帧）');
disposeFail();
await flush();
assert.deepEqual(kindsOf('unsubscribe'), ['logs'], '最后一个订阅者退出 → 发退订帧');
sentFrames.length = 0;

// ---- 场景 4：退订后立即重订阅（同一 tick 内意图删了又加，无 stale 帧） ----
manager.disconnect();
const disposeSub1 = manager.subscribeToLogs(() => {});
await flush();
const reopening = manager.connect();
await flush();
disposeSub1(); // open 前退订 → 意图删除，无 unsubscribe 帧
const disposeResub = manager.subscribeToLogs(() => {}); // 立即重订阅 → 意图重建
serverOpen();
await flush();
await reopening;
assert.deepEqual(kindsOf('subscribe'), ['logs'], '重开后按重建意图同步恰一条 subscribe');
assert.deepEqual(kindsOf('unsubscribe'), [], '全程无 stale unsubscribe 帧');
disposeResub();
await flush();
assert.deepEqual(kindsOf('unsubscribe'), ['logs'], '最终退订仍生效');
sentFrames.length = 0;

// ---- 场景 4b：同步构造抛错 → connect reject settle 且不缓存旧拒绝；后续重连可恢复 ----
manager.disconnect();
failNextConstructions = 1;
const disposedBoom = manager.subscribeToLogs(() => {});
await flush();
await connectAndOpen();
assert.deepEqual(kindsOf('subscribe'), ['logs'], '构造抛错恢复后 open 补发活性订阅');
disposedBoom();
await flush();
sentFrames.length = 0;

// ---- 场景 5：disconnect 后不自动重连 ----
manager.disconnect();
const socketsBefore = constructedSockets;
await new Promise((r) => setTimeout(r, 1300)); // 跨过首次重连延迟（1000ms）
assert.equal(constructedSockets, socketsBefore, 'disconnect 后跨过重连延迟仍无新连接');
assert.equal(unhandledCount, 0, '全程无 unhandled rejection');

console.log('ws 订阅回归检查全部通过');
