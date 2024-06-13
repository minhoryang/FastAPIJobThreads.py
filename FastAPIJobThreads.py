"""FastAPI(lifespan=lifespan_for_init_queue_and_threads) for seperating job threads.

Usage:
    app = FastAPI(lifespan=lifespan_for_init_queue_and_threads)

    @app.post("/")
    async def _(
        request: Request,
    ):
        # await async_long_func(...)
        await request.app.state.queueing(async_long_func(...))

    # THREADS=40 uvicorn app:app --workers 4 --limit-concurrency 160 --port 5555
    ## limit_concurrency=THREADS*WORKERS
    ## THREADS <- min(max(request_time) * 2)
"""

import asyncio
import os
from contextlib import asynccontextmanager
from queue import Queue
from threading import Thread

from uvloop import new_event_loop
from fastapi import FastAPI


def _job_thread_with_async_loop(queue: Queue):
    loop = new_event_loop()
    asyncio.set_event_loop(loop)

    async def _job_thread(queue: Queue):
        while True:
            try:
                _ = queue.get()
                coroutine = _[0]  # type: asyncio.coroutine
                future = _[1]  # type: asyncio.Future
                result = await coroutine
                future.set_result(result)
            finally:
                queue.task_done()

    loop.run_until_complete(_job_thread(queue))
    loop.close()


@asynccontextmanager
async def lifespan_for_init_queue_and_threads(app: FastAPI):
    queue = Queue()

    async def _queueing(coroutine):
        future = asyncio.get_event_loop().create_future()
        queue.put((coroutine, future))
        return await future

    app.state.queuing = _queueing

    threads = []
    _nthreads = int(os.environ.get('THREADS')) if os.environ.get('THREADS') else 20
    for _ in range(_nthreads):
        thread = Thread(target=_job_thread_with_async_loop, args=[queue,])
        threads.append(thread)
        thread.daemon = True
        thread.start()

    yield
