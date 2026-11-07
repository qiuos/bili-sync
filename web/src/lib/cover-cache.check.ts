// cover-cache 可运行回归检查：bun run src/lib/cover-cache.check.ts
// 覆盖场景：
// 1) pending 期间多个使用者共享同一请求，resolve 后引用计数结转，逐个卸载才 revoke
// 2) revoke 后再次 acquire 触发重新拉取，互不串用
// 3) aborted 旧请求既不创建 URL，其 finally 也不误删同 ID 的新请求
import { acquireCover, releaseCover, setCoverBlobFetcher } from './cover-cache';

const createdUrls: string[] = [];
const revokedUrls: string[] = [];
const realCreate = URL.createObjectURL.bind(URL);
const realRevoke = URL.revokeObjectURL.bind(URL);
URL.createObjectURL = (blob: Blob | MediaSource) => {
	const url = realCreate(blob);
	createdUrls.push(url);
	return url;
};
URL.revokeObjectURL = (url: string) => {
	revokedUrls.push(url);
	realRevoke(url);
};

const coverBlob = new Blob(['cover'], { type: 'image/jpeg' });
const resolvers: Array<(blob: Blob) => void> = [];
setCoverBlobFetcher(() => {
	return new Promise<Blob>((resolve) => {
		resolvers.push(resolve);
	});
});

let failed = false;
function assert(condition: boolean, message: string): void {
	if (condition) {
		console.log(`  ok: ${message}`);
	} else {
		failed = true;
		console.error(`  FAIL: ${message}`);
	}
}

async function settle(): Promise<void> {
	await new Promise<void>((resolve) => setTimeout(resolve, 0));
}

function resolveOldest(): void {
	const resolve = resolvers.shift();
	if (!resolve) throw new Error('没有待 resolve 的请求');
	resolve(coverBlob);
}

// 场景1：pending 期间两个使用者共享请求，resolve 后计数结转
console.log('场景1：引用计数结转');
const first = acquireCover(1);
const _second = acquireCover(1);
assert(first.url === null && first.pending !== null, '首次 acquire 进入 pending');
resolveOldest();
await settle();
assert(createdUrls.length === 1, '仅创建一次 object URL');
const cached = acquireCover(1);
const cachedUrl = cached.url;
assert(cachedUrl !== null && cached.pending === null, 'resolve 后命中缓存');
assert(resolvers.length === 0, '缓存命中不再发请求');
releaseCover(1);
assert(cachedUrl !== null && !revokedUrls.includes(cachedUrl), '仍有使用者时不 revoke');
releaseCover(1);
assert(cachedUrl !== null && !revokedUrls.includes(cachedUrl), '倒数第二个使用者卸载时不 revoke');
releaseCover(1);
assert(cachedUrl !== null && revokedUrls.includes(cachedUrl), '最后一个使用者卸载时 revoke');
assert(revokedUrls.length === 1, '恰好 revoke 一次');

// 场景2：revoke 后再次 acquire 触发重新拉取
console.log('场景2：revoke 后重新拉取');
const reused = acquireCover(2);
assert(reused.url === null && reused.pending !== null, '缓存已清空，重新发请求');
resolveOldest();
await settle();
assert(createdUrls.length === 2, '新请求创建新 URL');
releaseCover(2);
assert(revokedUrls.length === 2, '新 URL 用完即 revoke');

// 场景3：aborted 旧请求不创建 URL，也不误删同 ID 的新请求
console.log('场景3：aborted 旧请求与新请求并发');
const _abandoned = acquireCover(3);
releaseCover(3); // 全部卸载，旧请求被标记 aborted
const _replacement = acquireCover(3); // 立刻发起新请求
assert(resolvers.length === 2, '新请求已发出');
resolveOldest(); // 旧请求此刻才 resolve
await settle();
assert(createdUrls.length === 2, 'aborted 旧请求不创建 URL');
const another = acquireCover(3);
assert(resolvers.length === 1, 'acquire 复用同一新请求（旧请求 finally 未误删）');
resolveOldest(); // 新请求 resolve
await settle();
assert(createdUrls.length === 3, '新请求完成后创建 URL');
// acquireCover 的 url 字段由调用方消费 pending 后持有，此处模拟组件等待
const anotherUrl = another.url ?? (await another.pending);
assert(anotherUrl !== null, '新请求完成后返回 URL');
releaseCover(3);
assert(anotherUrl !== null && !revokedUrls.includes(anotherUrl), '计数未提前归零');
releaseCover(3);
assert(anotherUrl !== null && revokedUrls.includes(anotherUrl), '全部卸载后 revoke');

// 场景4：对未知 id 释放是无害空操作
console.log('场景4：未知 id 释放');
releaseCover(999);
assert(revokedUrls.length === 3, '未知 id 释放不产生副作用');

setCoverBlobFetcher(null);
URL.createObjectURL = realCreate;
URL.revokeObjectURL = realRevoke;

console.log(failed ? '回归检查失败' : '回归检查全部通过');
if (failed) throw new Error('cover-cache 回归检查失败');
