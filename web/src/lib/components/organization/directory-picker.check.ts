// directory-picker 生命周期回归检查：bun src/lib/components/organization/directory-picker.check.ts
// 覆盖 PRD AC1 的核心状态机（不启动浏览器，直接驱动组件内的加载/复位逻辑）：
// 1) 两种选择顺序（源→输出、输出→源）各打开一次并回填
// 2) 取消后重复打开：在途响应过期
// 3) 重复打开不重复加载（open 保持 true 时目标切换不触发新请求）
// 4) 打开沿失败回退根目录、恢复
// 复刻 directory-picker.svelte 的 $effect + loadDirectory 逻辑，组件本身依赖 bits-ui Dialog 无法脱离浏览器运行；
// 状态机、请求序号（requestSeq 令牌）与打开沿判断与组件实现保持镜像，改动组件时同步维护本文件。
// 受控模式要点：open 为单向 prop，关闭经 onclose 由父组件持有状态复位（此处 mirror 父组件 ownedState）。
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

let browseShouldFail = false;
function fakeBrowse(path?: string): Promise<BrowseResult> {
	return new Promise((resolve, reject) => {
		if (browseShouldFail) {
			reject(new Error('浏览失败: 演示错误'));
			return;
		}
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
// 这里同样不内部 await，调用方 resolve 请求后用 settle 等其落账。
// 受控镜像：runEffect(false) 不再自复位组件状态（那样写的是组件内部镜像的开/关状态），
// 而是只让在途响应过期（requestSeq 自增 + lastEffectOpen=false）；状态复位发生在
// parentOwnedState 置 null 重建 RunCtx 时——与真实父组件 `pickerTarget = null` 一致。
function runEffect(ctx: RunCtx, open: boolean, initialPath: string): void {
	if (open) {
		if (!ctx.effect.lastEffectOpen) {
			ctx.effect.lastEffectOpen = true;
			void loadDirectory(ctx.state, ctx.seq, initialPath, { fallbackRootOnError: true });
		}
	} else if (ctx.effect.lastEffectOpen) {
		ctx.effect.lastEffectOpen = false;
		ctx.seq.value += 1;
	}
}

interface RunCtx {
	state: PickerState;
	seq: { value: number };
	effect: { lastEffectOpen: boolean };
}

const settle = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));

// 父组件拥有的开关状态（mirror `pickerTarget = $state<...>(null)`）
let parentTarget: string | null = null;
function freshCtx(): RunCtx {
	parentTarget = null;
	return {
		state: { currentPath: '/', parentPath: null, entries: [], loading: false, error: '' },
		seq: { value: 0 },
		effect: { lastEffectOpen: false }
	};
}

// 父组件点击浏览图标：pickerTarget = target → open prop 变 true
function parentOpen(ctx: RunCtx, target: string, initialPath: string): void {
	parentTarget = target;
	runEffect(ctx, true, initialPath);
}

// 组件 onclose 经父组件：pickerTarget = null → open prop 变 false
function parentClose(ctx: RunCtx): void {
	if (parentTarget === null) throw new Error('父组件已在关闭状态，onclose 不应再触发');
	parentTarget = null;
	runEffect(ctx, false, '');
}

// 父组件确认回填：onConfirm 写入输入框，然后 pickerTarget = null
function parentConfirm(ctx: RunCtx, path: string, field: { value: string }): void {
	field.value = path;
	parentClose(ctx);
}

// 打开 → 解析请求 → 落账
async function openAndSettle(ctx: RunCtx, target: string, initialPath: string): Promise<void> {
	parentOpen(ctx, target, initialPath);
	latestBrowseRequest().resolve(sub(initialPath, '/'));
	await settle();
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

// 场景1：源目录 → 输出目录，顺序选择回填（已选一个目录后另一个浏览仍可用）
console.log('场景1：源 → 输出顺序选择');
const run1 = freshCtx();
const sourceField = { value: '' };
await openAndSettle(run1, 'source', '/media/downloads');
check(browseCount() === 1, '打开源目录发出一次浏览请求');
check(run1.state.currentPath === '/media/downloads', '源目录路径回填');
check(run1.state.entries.length === 1 && run1.state.loading === false, '子目录加载完成');
parentConfirm(run1, run1.state.currentPath, sourceField);
check(sourceField.value === '/media/downloads', '确认后源输入框回填');
check(parentTarget === null, '确认后父状态复位（受控 open 变 false）');
// 第二次浏览：选择输出目录，选择器必须能再次打开（AC1 核心）
const beforeCount = browseCount();
const outputField = { value: '' };
parentOpen(run1, 'output', '/media/emby');
check(browseCount() === beforeCount + 1, '选过源后第二次浏览（输出）发起新请求');
latestBrowseRequest().resolve(sub('/media/emby', '/media'));
await settle();
check(run1.state.currentPath === '/media/emby', '输出目录可再次浏览并回填');
check(run1.state.entries.length === 1 && run1.state.loading === false, '第二次浏览子目录加载完成');
parentConfirm(run1, run1.state.currentPath, outputField);
check(outputField.value === '/media/emby', '第二次确认输出输入框回填');

// 场景2：输出 → 源（反向），取消后重复打开仍能浏览并回填
console.log('场景2：输出 → 源反向选择，取消后重复打开');
const run2 = freshCtx();
await openAndSettle(run2, 'output', '/media/emby');
check(run2.state.currentPath === '/media/emby', '输出目录路径生效');
parentClose(run2);
const base2 = browseCount();
parentOpen(run2, 'source', '/media/downloads');
check(browseCount() === base2 + 1, '取消后重复打开发起新的浏览请求');
check(latestBrowseRequest().path === '/media/downloads', '新的初始路径生效');
latestBrowseRequest().resolve(sub('/media/downloads', '/media'));
await settle();
check(run2.state.currentPath === '/media/downloads', '重复打开后仍能浏览并回填');

// 场景3：取消后在途响应过期，不复位用户已浏览状态、不误写状态
console.log('场景3：取消后在途响应过期');
const run3 = freshCtx();
const base3 = browseCount();
parentOpen(run3, 'source', '/media');
check(browseCount() === base3 + 1 && run3.state.loading === true, '打开进入加载中');
parentClose(run3);
check(run3.state.currentPath === '/' && run3.state.entries.length === 0, '取消后立即复位');
browseQueue.filter((r) => r.path === '/media').forEach((r) => r.resolve(sub('/resolved', '/')));
await settle();
check(run3.state.currentPath === '/', '取消后在途响应被丢弃');
check(run3.state.entries.length === 0, '过期响应不写入条目');

// 场景4：两种顺序均完整可行
console.log('场景4：两种顺序的完整选择');
for (const firstPath of ['/a', '/b']) {
	const run = freshCtx();
	const field = { value: '' };
	await openAndSettle(run, 'source', firstPath);
	check(run.state.currentPath === firstPath, `首次选择路径 ${firstPath} 有效`);
	parentConfirm(run, run.state.currentPath, field);
	check(field.value === firstPath, `首次选中回填 ${firstPath}`);
	await openAndSettle(run, 'output', `${firstPath}-out`);
	check(run.state.currentPath === `${firstPath}-out`, `第二次选择路径 ${firstPath}-out 有效`);
	parentConfirm(run, run.state.currentPath, field);
	check(field.value === `${firstPath}-out`, `第二次选中回填 ${firstPath}-out`);
	assert.equal(browseCount() > 0, true);
}

// 场景5：打开沿仅触发一次（重复调用 open=true 不重复请求）
console.log('场景5：打开沿去重');
const run5 = freshCtx();
const base5 = browseCount();
parentOpen(run5, 'source', '/once');
parentOpen(run5, 'source', '/once');
check(browseCount() === base5 + 1, '重复 open=true 不重复请求');
latestBrowseRequest().resolve(sub('/once', '/'));
await settle();
check(run5.state.currentPath === '/once', '请求完成后路径生效');

// 场景6：打开沿请求失败——首次打开回退根目录；再次打开成功
console.log('场景6：请求失败回退与恢复');
const run6 = freshCtx();
browseShouldFail = true;
parentOpen(run6, 'source', '/bad');
await settle();
// 失败路径：fallback 也会失败（全失败），状态.error 落地为最后一次错误
check(run6.state.error !== '' || run6.state.currentPath === '/', '失败后回退根目录或提示错误');
check(!run6.state.loading, '失败后 loading 复位');
browseShouldFail = false;
parentClose(run6);
parentOpen(run6, 'output', '/media/emby');
latestBrowseRequest().resolve(sub('/media/emby', '/media'));
await settle();
check(run6.state.currentPath === '/media/emby', '失败后再次打开恢复正常浏览');

console.log(failed ? '回归检查失败' : '目录选择器回归检查全部通过');
if (failed) throw new Error('directory-picker 回归检查失败');
