app = FastAPI(lifespan=lifespan_for_init_queue_and_threads)

@app.post("/")
async def _(
    request: Request,
):
    # await async_long_func(...)
    await request.app.state.queueing(async_long_func(...))
