"""WorkerPool 单元测试：FakeWorker，不碰真实浏览器。"""
import asyncio

import pytest

from app.config import Config
from app.pool import QueueFull, WorkerPool
from app.schemas import TaskStatus, WorkerState
from app.store import TaskStore


class FakeWorker:
    """模拟 worker：被指派后立即把任务标记完成。"""

    def __init__(self, worker_id, site, store):
        self.worker_id = worker_id
        self.site = site
        self.model = ""
        self.state = WorkerState.idle
        self.detail = None
        self.current_task_id = None
        self.done_count = 0
        self.fail_count = 0
        self.store = store
        self.ran = []
        self._task = None

    async def start(self):
        pass

    async def stop(self):
        if self._task and not self._task.done():
            self._task.cancel()

    def start_task(self, task):
        self.state = WorkerState.busy
        self.current_task_id = task.task_id
        self._task = asyncio.create_task(self.run_task(task))

    async def run_task(self, task):
        self.store.mark_running(task.task_id)
        await asyncio.sleep(0.01)  # 模拟执行耗时
        self.ran.append(task.task_id)
        self.store.mark_done(task.task_id, f"答案 from {self.worker_id}")
        self.done_count += 1
        self.current_task_id = None
        self.state = WorkerState.idle


def make_pool(worker_sites=(), queue_size=100, history_size=10):
    config = Config.model_validate({
        "workers": [],  # worker 由测试注入
        "queue": {"max_size": queue_size},
        "dashboard": {"history_size": history_size},
    })
    store = TaskStore(history_size)
    pool = WorkerPool(config, store, client=None, dispatch_interval=0.01)
    for i, site in enumerate(worker_sites, 1):
        pool.workers.append(FakeWorker(f"w{i}", site, store))
    return pool, store


async def wait_done(store, task_id, timeout=3.0):
    """轮询直到任务终态。"""
    for _ in range(int(timeout / 0.02)):
        status = store.get(task_id).status
        if status in (TaskStatus.done, TaskStatus.failed):
            return status
        await asyncio.sleep(0.02)
    raise AssertionError(f"任务 {task_id[:8]} 未在 {timeout}s 内完成")


async def test_submit_并入队():
    pool, store = make_pool(["kimi"])
    task_id = await pool.submit("你好", "kimi")
    task = store.get(task_id)
    assert task is not None
    assert task.status == TaskStatus.queued
    assert task.site == "kimi"
    assert pool.queue.qsize() == 1


async def test_dispatch_按site匹配():
    pool, store = make_pool(["kimi", "deepseek"])
    await pool.start()
    try:
        tid = await pool.submit("问 deepseek", "deepseek")
        assert await wait_done(store, tid) == TaskStatus.done
        assert pool.workers[1].ran == [tid]
        assert pool.workers[0].ran == []
        assert store.get(tid).result == "答案 from w2"
    finally:
        await pool.stop()


async def test_dispatch_site为None时任一空闲():
    pool, store = make_pool(["kimi"])
    await pool.start()
    try:
        tid = await pool.submit("任意站点", None)
        assert await wait_done(store, tid) == TaskStatus.done
        assert pool.workers[0].ran == [tid]
    finally:
        await pool.stop()


async def test_无空闲worker时排队_按序执行():
    pool, store = make_pool(["kimi"])
    await pool.start()
    try:
        t1 = await pool.submit("第一", None)
        t2 = await pool.submit("第二", None)
        await wait_done(store, t1)
        await wait_done(store, t2)
        # 单 worker 串行执行，顺序与提交一致
        assert pool.workers[0].ran == [t1, t2]
    finally:
        await pool.stop()


async def test_队列满抛QueueFull():
    pool, store = make_pool(["kimi"], queue_size=1)
    # worker 置忙，dispatcher 不会取走任务
    pool.workers[0].state = WorkerState.busy
    await pool.start()
    try:
        await pool.submit("一", None)
        with pytest.raises(QueueFull):
            await pool.submit("二", None)
        # 被拒绝的任务不留记录
        assert len(store._tasks) == 1
    finally:
        await pool.stop()


async def test_workers_info与queue快照():
    pool, store = make_pool(["kimi"])
    pool.workers[0].state = WorkerState.busy
    await pool.submit("排队中", None)
    infos = pool.workers_info()
    assert infos[0].worker_id == "w1"
    assert infos[0].site == "kimi"
    assert infos[0].state == WorkerState.busy
    snap = pool.queue_snapshot()
    assert len(snap) == 1
    assert snap[0].prompt_preview == "排队中"
