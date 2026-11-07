// 封面对象 URL 缓存：按挂载引用计数管理，最后一个使用方卸载时 revoke；
// 请求竞态：全部使用方卸载时若仍在拉取则标记 aborted，不再创建 object URL
// ponytail: 刻意用普通 Map 而非 SvelteMap，缓存变动不应触发组件 effect 重跑

interface CoverEntry {
	url: string;
	count: number;
}

interface CoverLoading {
	count: number;
	aborted: boolean;
	promise: Promise<string | null>;
}

const coverCache = new Map<number, CoverEntry>();
const coverLoading = new Map<number, CoverLoading>();

let blobFetcher: ((id: number) => Promise<Blob>) | null = null;

/** 仅供回归检查注入替身，生产代码传 null 使用默认 API 客户端 */
export function setCoverBlobFetcher(fetcher: ((id: number) => Promise<Blob>) | null): void {
	blobFetcher = fetcher;
}

function fetchCoverBlob(id: number): Promise<Blob> {
	if (blobFetcher) return blobFetcher(id);
	// 延迟引入 API 客户端，让回归检查可以脱离浏览器环境独立运行
	return import('./api').then(({ apiClient }) => apiClient.getVideoCoverBlob(id));
}

function getLoading(id: number): CoverLoading {
	let loading = coverLoading.get(id);
	if (!loading) {
		loading = {
			count: 0,
			aborted: false,
			promise: fetchCoverBlob(id)
				.then((blob) => {
					if (!loading || loading.aborted) return null;
					const objectUrl = URL.createObjectURL(blob);
					// 结转 pending 期间的引用计数，否则先到的使用者会提前触发 revoke
					coverCache.set(id, { url: objectUrl, count: loading.count });
					return objectUrl;
				})
				.catch(() => null)
				.finally(() => {
					// 仅当表中仍是本请求时才删除，避免 aborted 旧请求误删同 ID 的新请求
					if (coverLoading.get(id) === loading) {
						coverLoading.delete(id);
					}
				})
		};
		coverLoading.set(id, loading);
	}
	loading.count += 1;
	return loading;
}

export function acquireCover(id: number): {
	url: string | null;
	pending: Promise<string | null> | null;
} {
	const cached = coverCache.get(id);
	if (cached) {
		cached.count += 1;
		return { url: cached.url, pending: null };
	}
	return { url: null, pending: getLoading(id).promise };
}

export function releaseCover(id: number): void {
	const entry = coverCache.get(id);
	if (entry) {
		entry.count -= 1;
		if (entry.count <= 0) {
			URL.revokeObjectURL(entry.url);
			coverCache.delete(id);
		}
		return;
	}
	const pending = coverLoading.get(id);
	if (pending) {
		pending.count -= 1;
		if (pending.count <= 0) {
			pending.aborted = true;
			coverLoading.delete(id);
			pending.promise.then((objectUrl) => {
				if (objectUrl) URL.revokeObjectURL(objectUrl);
			});
		}
	}
}
