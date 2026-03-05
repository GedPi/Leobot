from __future__ import annotations

# Repeating background jobs: register_interval adds a job, start/stop run them with optional jitter and run_on_start.

import asyncio
import logging
import random
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict


log = logging.getLogger("leobot.scheduler")

JobFn = Callable[[], Awaitable[None]]


@dataclass(slots=True)
class Job:
    name: str
    interval_s: float
    fn: JobFn
    jitter_s: float = 0.0
    run_on_start: bool = False


# Holds named jobs; start() launches a task per job, stop() cancels and drains them.
class Scheduler:
    def __init__(self):
        self._jobs: Dict[str, Job] = {}
        self._tasks: Dict[str, asyncio.Task] = {}
        self._stop = asyncio.Event()

    # Registers a job to run every seconds (with optional jitter and run_on_start); name must be unique.
    def register_interval(
        self,
        name: str,
        seconds: float,
        fn: JobFn,
        *,
        jitter_seconds: float = 0.0,
        run_on_start: bool = False,
    ) -> None:
        name = name.strip()
        if not name:
            raise ValueError("Job name required")
        if seconds <= 0:
            raise ValueError("Interval must be > 0")
        if name in self._jobs:
            raise ValueError(f"Job already registered: {name}")
        self._jobs[name] = Job(
            name=name,
            interval_s=float(seconds),
            fn=fn,
            jitter_s=float(jitter_seconds),
            run_on_start=bool(run_on_start),
        )

    def list_jobs(self) -> list[str]:
        return sorted(self._jobs.keys())

    # Creates a task per registered job and clears stop event; no-op if already started.
    async def start(self) -> None:
        if self._tasks:
            return
        self._stop.clear()
        for name, job in self._jobs.items():
            self._tasks[name] = asyncio.create_task(self._runner(job), name=f"job:{name}")
        log.info("Scheduler started (%d jobs)", len(self._tasks))

    # Sets stop event, cancels all job tasks and waits for them to finish.
    async def stop(self) -> None:
        self._stop.set()
        for t in list(self._tasks.values()):
            t.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        self._tasks.clear()
        log.info("Scheduler stopped")

    async def _runner(self, job: Job) -> None:
        try:
            if job.run_on_start:
                await self._run_once(job)

            while not self._stop.is_set():
                sleep_s = job.interval_s
                if job.jitter_s > 0:
                    sleep_s += random.uniform(0, job.jitter_s)

                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=sleep_s)
                except asyncio.TimeoutError:
                    pass

                if self._stop.is_set():
                    break

                await self._run_once(job)
        except asyncio.CancelledError:
            return

    async def _run_once(self, job: Job) -> None:
        start = time.time()
        try:
            await job.fn()
        except Exception:
            log.exception("Job failed: %s", job.name)
        finally:
            dt = time.time() - start
            if dt > 2.0:
                log.info("Job %s ran in %.2fs", job.name, dt)
