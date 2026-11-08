import { toast } from 'svelte-sonner';
import api from './api';
import type { SysInfo, TaskStatus } from './types';

// 支持的事件类型
export enum EventType {
	Logs = 'logs',
	Tasks = 'tasks',
	SysInfo = 'sysInfo'
}

// 服务器事件响应格式
interface ServerEvent {
	logs?: string;
	tasks?: TaskStatus;
	sysInfo?: SysInfo;
}

// 客户端事件请求格式
interface ClientEvent {
	subscribe?: EventType;
	unsubscribe?: EventType;
}

// 回调函数类型定义
type LogsCallback = (data: string) => void;
type TasksCallback = (data: TaskStatus) => void;
type SysInfoCallback = (data: SysInfo) => void;

export class WebSocketManager {
	private static instance: WebSocketManager;
	private socket: WebSocket | null = null;
	private connected = false;
	private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
	private reconnectAttempts = 0;
	private baseReconnectDelay = 1000;

	private logsSubscribers: Set<LogsCallback> = new Set();
	private tasksSubscribers: Set<TasksCallback> = new Set();
	private sysInfoSubscribers: Set<SysInfoCallback> = new Set();

	private subscribedEvents: Set<EventType> = new Set();
	private connectionPromise: Promise<void> | null = null;
	/** 用户主动断开（退出认证）后不自动重连 */
	private disconnectedByUser = false;

	private constructor() {}

	public static getInstance(): WebSocketManager {
		if (!WebSocketManager.instance) {
			WebSocketManager.instance = new WebSocketManager();
		}
		return WebSocketManager.instance;
	}

	// 连接 WebSocket；失败一定 settle（open 前关闭由 onclose 落锤 reject）
	public connect(): Promise<void> {
		if (this.connected) return Promise.resolve();
		if (this.connectionPromise) return this.connectionPromise;

		this.disconnectedByUser = false;
		// 同步构造异常时 executor 已先行落锤（已把 connectionPromise 置回 null 并安排重连）；
		// 此时不能把本代已拒绝的 promise 覆写回字段，否则永久缓存旧拒绝结果、重连永不建连
		let settledSync = false;
		const promise = new Promise<void>((resolve, reject) => {
			let settled = false;
			const settleResolve = () => {
				if (settled) return;
				settled = true;
				settledSync = true;
				this.connectionPromise = null;
				resolve();
			};
			const settleReject = (error: unknown) => {
				if (settled) return;
				settled = true;
				settledSync = true;
				this.connectionPromise = null;
				reject(error);
			};

			try {
				const token = api.getAuthToken() || '';
				const protocol = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
				// 使用 base64URL no padding 编码 token 以避免特殊字符问题
				this.socket = new WebSocket(
					`${protocol}${window.location.host}/api/ws`,
					btoa(token).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
				);
				this.socket.onopen = () => {
					this.connected = true;
					this.reconnectAttempts = 0;
					// 连接恢复即取消已排期的重连，避免旧定时器重复建连
					if (this.reconnectTimer !== null) {
						clearTimeout(this.reconnectTimer);
						this.reconnectTimer = null;
					}
					// 每代连接统一以当前有消费者的意图同步一次，不排队旧控制帧
					this.resubscribeEvents();
					settleResolve();
				};

				this.socket.onmessage = this.handleMessage.bind(this);

				this.socket.onclose = () => {
					this.connected = false;
					// open 前关闭：给挂起的 connect()（sendMessage 的 await 链路）一个确定结果
					settleReject(new Error('WebSocket closed before open'));
					this.scheduleReconnect();
				};

				this.socket.onerror = (error) => {
					console.error('WebSocket error:', error);
					toast.error('WebSocket 连接发生错误，请稍后重试');
				};
			} catch (error) {
				settleReject(error);
				console.error('Failed to create WebSocket:', error);
				toast.error('创建 WebSocket 连接失败，请检查网络或稍后重试');
				this.scheduleReconnect();
			}
		});

		// 同步落锤（构造抛错）后字段已被 executor 置空：短路重连依赖已排期的 scheduleReconnect，
		// 不把已拒绝的 promise 写回，避免后续重连永远复用旧拒绝结果
		if (!settledSync) {
			this.connectionPromise = promise;
		}

		return promise;
	}

	private handleMessage(event: MessageEvent): void {
		try {
			const data = JSON.parse(event.data) as ServerEvent;

			if (data.logs !== undefined) {
				this.notifyLogsSubscribers(data.logs);
			} else if (data.tasks !== undefined) {
				this.notifyTasksSubscribers(data.tasks);
			} else if (data.sysInfo !== undefined) {
				this.notifySysInfoSubscribers(data.sysInfo);
			}
		} catch (error) {
			console.error('Failed to parse WebSocket message:', error, event.data);
			toast.error('解析 WebSocket 消息失败', {
				description: `消息内容：${event.data}\n错误信息：${error instanceof Error ? error.message : String(error)}`
			});
		}
	}

	// 事件当前是否有消费者（回调集合非空）
	private hasCallbacks(eventType: EventType): boolean {
		switch (eventType) {
			case EventType.Logs:
				return this.logsSubscribers.size > 0;
			case EventType.Tasks:
				return this.tasksSubscribers.size > 0;
			case EventType.SysInfo:
				return this.sysInfoSubscribers.size > 0;
		}
	}

	// 仅在已连接时同步发送控制帧；未连接直接抛出（subscribe/unsubscribe 自行守卫，
	// onopen 的统一同步负责连接中登记的意图——不排队旧控制帧）
	private sendMessage(message: ClientEvent): void {
		if (!this.connected || !this.socket) {
			throw new Error('WebSocket not connected');
		}

		try {
			this.socket.send(JSON.stringify(message));
		} catch (error) {
			console.error('Failed to send message:', error);
			toast.error('发送 WebSocket 消息失败', {
				description: `消息内容：${JSON.stringify(message)}\n错误信息：${error instanceof Error ? error.message : String(error)}`
			});
		}
	}

	// 订阅事件：意图先行登记；未连接只登记并触发连接，open 后统一同步当前消费者（不排队旧帧）
	private async subscribe(eventType: EventType): Promise<void> {
		if (this.subscribedEvents.has(eventType)) return;
		this.subscribedEvents.add(eventType);
		if (!this.connected) {
			// 触发连接；open 时 resubscribeEvents 统一同步，失败由 onclose 走重连
			this.connect().catch(() => {});
			return;
		}
		try {
			this.sendMessage({ subscribe: eventType });
		} catch (error) {
			// 发送失败：意图保留，重连 open 后统一补发
			console.error(`Failed to subscribe ${eventType}:`, error);
		}
	}

	// 取消订阅事件：意图立即删除（避免掉线后无消费者仍被同步恢复推送）；未连接无需发帧
	private async unsubscribe(eventType: EventType): Promise<void> {
		if (!this.subscribedEvents.has(eventType)) return;
		this.subscribedEvents.delete(eventType);
		if (!this.connected) return;
		try {
			this.sendMessage({ unsubscribe: eventType });
		} catch (error) {
			console.error(`Failed to unsubscribe ${eventType}:`, error);
		}
	}

	private async resubscribeEvents(): Promise<void> {
		for (const eventType of this.subscribedEvents) {
			// 只同步当前有消费者的意图；连接中退订的残留意图在此被过滤
			if (this.hasCallbacks(eventType)) {
				try {
					this.sendMessage({ subscribe: eventType });
				} catch (error) {
					console.error(`Failed to resubscribe ${eventType}:`, error);
				}
			}
		}
	}

	// 指数退避重连，不设总数上限（上限 30s），保证长时间断网/后端重启后仍能恢复推送（R3）
	private scheduleReconnect(): void {
		if (this.disconnectedByUser) return;

		if (this.reconnectTimer !== null) {
			clearTimeout(this.reconnectTimer);
		}

		// 指数退避重连（1s 起步，封顶 30s），不限制尝试次数；onopen 时计数归零
		const delay = Math.min(this.baseReconnectDelay * Math.pow(2, this.reconnectAttempts), 30000);
		console.log(`Scheduling reconnect in ${delay}ms`);

		this.reconnectTimer = setTimeout(() => {
			this.reconnectAttempts++;
			// 重连链 fire-and-forget：connect 失败会 settle，catch 消掉 rejection，下一次由 onclose 再排
			this.connect().catch(() => {});
		}, delay);
	}

	public subscribeToLogs(callback: LogsCallback): () => void {
		this.logsSubscribers.add(callback);

		if (this.logsSubscribers.size === 1) {
			this.subscribe(EventType.Logs);
		}

		return () => {
			this.logsSubscribers.delete(callback);
			if (this.logsSubscribers.size === 0) {
				this.unsubscribe(EventType.Logs);
			}
		};
	}

	// 订阅任务状态
	public subscribeToTasks(callback: TasksCallback): () => void {
		this.tasksSubscribers.add(callback);

		if (this.tasksSubscribers.size === 1) {
			this.subscribe(EventType.Tasks);
		}

		return () => {
			this.tasksSubscribers.delete(callback);
			if (this.tasksSubscribers.size === 0) {
				this.unsubscribe(EventType.Tasks);
			}
		};
	}

	public subscribeToSysInfo(callback: SysInfoCallback): () => void {
		this.sysInfoSubscribers.add(callback);

		if (this.sysInfoSubscribers.size === 1) {
			this.subscribe(EventType.SysInfo);
		}

		return () => {
			this.sysInfoSubscribers.delete(callback);
			if (this.sysInfoSubscribers.size === 0) {
				this.unsubscribe(EventType.SysInfo);
			}
		};
	}

	private notifyLogsSubscribers(data: string): void {
		this.logsSubscribers.forEach((callback) => {
			try {
				callback(data);
			} catch (error) {
				console.error('Error in logs subscriber callback:', error);
			}
		});
	}

	private notifyTasksSubscribers(data: TaskStatus): void {
		this.tasksSubscribers.forEach((callback) => {
			try {
				callback(data);
			} catch (error) {
				console.error('Error in tasks subscriber callback:', error);
			}
		});
	}

	private notifySysInfoSubscribers(data: SysInfo): void {
		this.sysInfoSubscribers.forEach((callback) => {
			try {
				callback(data);
			} catch (error) {
				console.error('Error in sysInfo subscriber callback:', error);
			}
		});
	}

	public disconnect(): void {
		this.disconnectedByUser = true;

		if (this.socket) {
			this.socket.close();
			this.socket = null;
		}

		if (this.reconnectTimer !== null) {
			clearTimeout(this.reconnectTimer);
			this.reconnectTimer = null;
		}

		this.connected = false;
		this.connectionPromise = null;
		// 清空意图的同时清空回调注册：回调持有方（页面卸载/重新认证）会重新订阅，
		// 否则会出现「有回调无意图」→ 重开后不恢复推送的不变式缺口
		this.subscribedEvents.clear();
		this.logsSubscribers.clear();
		this.tasksSubscribers.clear();
		this.sysInfoSubscribers.clear();
	}
}

export const wsManager = WebSocketManager.getInstance();
