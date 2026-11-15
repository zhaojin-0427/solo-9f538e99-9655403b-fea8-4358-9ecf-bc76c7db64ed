"""运行期操作闸门：为“停机恢复切换”提供停机窗口。

正常业务读写（更正、查询、同步）都持有共享读锁；恢复确认时取得独占写锁：
  1. 先将闸门置为 paused —— 新到的业务请求立即得到可重试的暂停错误；
  2. 等待已经受理的读操作全部结束（在途事务完整提交或回滚）；
  3. 独占期间替换两份库文件，期间没有任何本应用的数据库连接；
  4. 释放闸门并递增 generation，后台同步/更正线程按新库状态继续。

这样既满足“已受理操作结束、新操作暂停”，也保证旧执行者（旧一代）不会
在恢复后的库上留下写入：任何一次跨“读源库 → 写目标库”的操作都完整落在
同一个读区段内，切换只会在区段之间发生。
"""
import threading
import time
from contextlib import contextmanager


class OperationPaused(Exception):
    """恢复切换窗口内到达的新业务操作；调用方应返回可重试错误。"""


class StorageUnavailable(Exception):
    """上一次两库切换未解决，存储配对不完整；必须暂停所有业务读写。"""


class OperationGate:
    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._readers = 0
        self._writer = False
        self._paused = False
        self._storage_ok = True
        self._generation = 0

    # ------------------------------------------------------------ 观测
    @property
    def paused(self) -> bool:
        with self._cond:
            return self._paused

    @property
    def storage_ok(self) -> bool:
        with self._cond:
            return self._storage_ok

    @property
    def generation(self) -> int:
        with self._cond:
            return self._generation

    @property
    def readers(self) -> int:
        with self._cond:
            return self._readers

    def set_storage_ok(self, ok: bool) -> None:
        with self._cond:
            self._storage_ok = ok
            self._cond.notify_all()

    # ------------------------------------------------------------ 读区段
    @contextmanager
    def read(self, kind: str = "operation"):
        """进入一个业务读区段。paused / 存储不可用时立即抛出。"""
        with self._cond:
            if not self._storage_ok:
                raise StorageUnavailable(
                    "两库处于未解决的切换状态，业务读写已暂停，请先在恢复页处理"
                )
            if self._paused or self._writer:
                raise OperationPaused("停机恢复切换进行中，请稍后重试")
            self._readers += 1
        try:
            yield
        finally:
            with self._cond:
                self._readers -= 1
                if self._readers == 0:
                    self._cond.notify_all()

    # ------------------------------------------------------------ 独占区段
    @contextmanager
    def exclusive(self, timeout: float | None = None, drain_hook=None):
        """申请恢复切换独占权。

        先置 paused 拒绝新业务，再等待在途读区段清空。超时未清空则自动
        撤销 paused，保持原库继续服务；不产生任何文件改动。
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        acquired = False
        with self._cond:
            self._paused = True
            self._cond.notify_all()
            try:
                while self._readers > 0 or self._writer:
                    remaining = None
                    if deadline is not None:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise TimeoutError(
                                f"等待在途操作结束超时（仍有 {self._readers} 个）"
                            )
                    self._cond.wait(remaining)
                self._writer = True
                acquired = True
            finally:
                if not acquired:
                    self._paused = False
                    self._cond.notify_all()
        try:
            if drain_hook is not None:
                drain_hook()
            yield
        finally:
            with self._cond:
                self._writer = False
                self._paused = False
                self._generation += 1
                self._cond.notify_all()
