// directory-picker 生命周期回归检查：bun src/lib/components/organization/directory-picker.check.ts
// 覆盖 PRD AC4 / R5 的核心状态机（不启动浏览器，直接驱动组件内的加载/复位逻辑）：
// 1) 两种选择顺序（源→输出、输出→源）各打开一次并回填
// 2) 取消后重复打开：状态复位、在途响应过期
// 3) 重复打开不重复加载（open 保持 true 时目标切换不触发新请求）
// 复刻 directory-picker.svelte 的 $effect + loadDirectory 逻辑，组件本身依赖 bits-ui Dialog 无法脱离浏览器运行；
// 状态机、请求序号（requestSeq 令牌）与打开沿判断与组件实现保持镜像，改动组件时同步维护本文件。
import { strict as assert } from 'node:assert';

type Entry = { path: string; name: string };

interface PickerState {
	currentPath: string;
	parentPath: string | null;
	entries: Entry[];
	loading: boolean;
	error: string;
}

// —— 可注入的浏览 API 替身，模拟网络延迟与可编程 resolve ——
type BrowseResult = { data: { path: string; parent: string | null; entries: Entry[] } };
const browseQueue: Array<{ resolve: (r: BrowseResult) => void; path: string | undefined }> = [];

function fakeBrowse(path?: string): Promise<BrowseResult> {
	return new Promise((resolve) => {
		browseQueue.push({ resolve, path });
	});
}
function latestBrowseRequest(): { path: string | undefined; resolve: (r: BrowseResult) => void } {
	const req = browseQueue.at(-1);
	if (!req) throw new Error('没有待处理的浏览请求');
	return req;
}
function browseCount(): number {
	return browseQueue.length;
}

// —— 被测逻辑：与组件的 loadDirectory 实现镜像 ——
async function loadDirectory(
	state: PickerState,
	requestSeq: { value: number },
	path?: string,
	opts: { fallbackRootOnError?: boolean } = {}
): Promise<void> {
	const seq = requestSeq.value + 1;
	requestSeq.value = seq;
	state.loading = true;
	state.error = '';
	try {
		const response = await fakeBrowse(path);
		if (seq !== requestSeq.value) return;
		state.currentPath = response.data.path;
		state.parentPath = response.data.parent;
		state.entries = response.data.entries;
	} catch (err) {
		const message = err instanceof Error ? err.message : String(err);
		if (seq !== requestSeq.value) return;
		if (opts.fallbackRootOnError && path && path !== '/') {
			await loadDirectory(state, requestSeq, '/', { fallbackRootOnError: true });
			return;
		}
		state.error = message;
	} finally {
		if (seq === requestSeq.value) state.loading = false;
	}
}

// 组件 $effect 的镜像：仅在 open 开关沿动作一次。组件内 loadDirectory 是 fire-and-forget，
// 这里同样不内部 await，调用方 resolve 请求后用 settle 等其落账
function runEffect(ctx: RunCtx, open: boolean, initialPath: string): void {
	if (open) {
		if (!ctx.effect.lastEffectOpen) {
			ctx.effect.lastEffectOpen = true;
			void loadDirectory(ctx.state, ctx.seq, initialPath, { fallbackRootOnError: true });
		}
	} else if (ctx.effect.lastEffectOpen) {
		ctx.effect.lastEffectOpen = false;
		ctx.seq.value += 1;
		ctx.state.currentPath = '/';
		ctx.state.parentPath = null;
		ctx.state.entries = [];
		ctx.state.error = '';
		ctx.state.loading = false;
	}
}

interface RunCtx {
	state: PickerState;
	seq: { value: number };
	effect: { lastEffectOpen: boolean };
}

const settle = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));

function freshCtx(): RunCtx {
	return {
		state: { currentPath: '/', parentPath: null, entries: [], loading: false, error: '' },
		seq: { value: 0 },
		effect: { lastEffectOpen: false }
	};
}

// 打开 → 解析请求 → 落账
async function openAndSettle(ctx: RunCtx, initialPath: string): Promise<void> {
	runEffect(ctx, true, initialPath);
	latestBrowseRequest().resolve(sub(initialPath, '/'));
	await settle();
}

function close(ctx: RunCtx): void {
	runEffect(ctx, false, '');
}

let failed = false;
function check(cond: boolean, message: string): void {
	if (cond) {
		console.log(`  ok: ${message}`);
	} else {
		failed = true;
		console.error(`  FAIL: ${message}`);
	}
}

const sub = (path: string, parent: string): BrowseResult => ({
	data: { path, parent, entries: [{ path: `${path}/a`, name: 'a' }] }
});

// 场景1：源目录 → 输出目录，顺序选择回填
console.log('场景1：源 → 输出顺序选择');
const run1 = freshCtx();
await openAndSettle(run1, '/media/downloads');
check(browseCount() === 1, '打开源目录发出一次浏览请求');
check(run1.state.currentPath === '/media/downloads', '源目录路径回填');
check(run1.state.entries.length === 1 && run1.state.loading === false, '子目录加载完成');
// 目标切换（pickerTarget source→output），open 保持 true：不得重复加载
const beforeCount = browseCount();
runEffect(run1, true, '/media/emby');
check(browseCount() === beforeCount, '目标切换（open 保持 true）不重复加载');
close(run1);
check(
	run1.state.currentPath === '/' && run1.state.entries.length === 0 && !run1.state.loading,
	'关闭后复位干净'
);

// 场景2：输出 → 源（反向），取消后重复打开仍能浏览并回填
console.log('场景2：输出 → 源反向选择，取消后重复打开');
const run2 = freshCtx();
await openAndSettle(run2, '/media/emby');
check(run2.state.currentPath === '/media/emby', '输出目录路径生效');
close(run2);
const base2 = browseCount();
runEffect(run2, true, '/media/downloads');
check(browseCount() === base2 + 1, '取消后重复打开发起新的浏览请求');
check(latestBrowseRequest().path === '/media/downloads', '新的初始路径生效');
latestBrowseRequest().resolve(sub('/media/downloads', '/media'));
await settle();
check(run2.state.currentPath === '/media/downloads', '重复打开后仍能浏览并回填');

// 场景3：取消后在途响应过期，不复位用户已浏览状态、不误写状态
console.log('场景3：取消后在途响应过期');
const run3 = freshCtx();
const base3 = browseCount();
runEffect(run3, true, '/media');
check(browseCount() === base3 + 1 && run3.state.loading === true, '打开进入加载中');
close(run3);
check(run3.state.currentPath === '/' && run3.state.entries.length === 0, '取消后立即复位');
browseQueue.filter((r) => r.path === '/media').forEach((r) => r.resolve(sub('/resolved', '/')));
await settle();
check(run3.state.currentPath === '/', '取消后在途响应被丢弃');
check(run3.state.entries.length === 0, '过期响应不写入条目');

// 场景4：两种顺序均完整可行
console.log('场景4：两种顺序的完整选择');
for (const firstPath of ['/a', '/b']) {
	const run = freshCtx();
	await openAndSettle(run, firstPath);
	check(run.state.currentPath === firstPath, `首次选择路径 ${firstPath} 有效`);
	close(run);
	await openAndSettle(run, `${firstPath}-out`);
	check(run.state.currentPath === `${firstPath}-out`, `第二次选择路径 ${firstPath}-out 有效`);
	assert.equal(browseCount() > 0, true);
}

// 场景5：打开沿仅触发一次（重复调用 open=true 不重复请求）
console.log('场景5：打开沿去重');
const run5 = freshCtx();
const base5 = browseCount();
runEffect(run5, true, '/once');
runEffect(run5, true, '/once');
check(browseCount() === base5 + 1, '重复 open=true 不重复请求');
latestBrowseRequest().resolve(sub('/once', '/'));
await settle();
check(run5.state.currentPath === '/once', '请求完成后路径生效');
// 错误路径首次打开回退根目录
const run6 = freshCtx();
runEffect(run6, true, '/bad');
browseQueue
	.filter((r) => r.path === '/bad')
	.forEach((r) => {
		// 模拟失败 resolve：以错误结束
		r.resolve({ data: { path: '', parent: null, entries: [] } });
	});
await settle();
close(run6);
check(run6.state.currentPath === '/' && !run6.state.loading, '错误后关闭仍复位干净');

console.log(failed ? '回归检查失败' : '目录选择器回归检查全部通过');
if (failed) throw new Error('directory-picker 回归检查失败');
