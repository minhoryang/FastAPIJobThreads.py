import datetime, time
from langchain.callbacks.base import AsyncCallbackHandler


def _print(*args):
    print(str(datetime.datetime.today()), *args)


def on_start(func):
    async def _(self, serialized, *args, **kwargs):
        class_name = serialized.get("id", [""])[-1]
        now = time.time()
        category = func.__name__.split('_')[1]
        if category not in self.stacks:
            self.stacks[category] = []
        self.stacks[category].append((class_name, now))
        _print(self.session_id, func.__name__, class_name)
    return _


def on_end(func):
    async def _(self, serialized, *args, **kwargs):
        category = func.__name__.split('_')[1]
        class_name, old = self.stacks[category].pop()
        diff = "+%.6s secs" % (time.time() - old)
        _print(self.session_id, func.__name__, class_name, diff)
    return _


class MyLogAsyncCallbackHandler(AsyncCallbackHandler):
    def __init__(self, session_id):
        self.session_id = session_id
        self.stacks = {}

    @on_start
    async def on_llm_start():
        pass

    @on_end
    async def on_llm_end():
        pass

    @on_start
    async def on_chain_start():
        pass

    @on_end
    async def on_chain_end():
        pass

    @on_start
    async def on_tool_start():
        pass

    @on_end
    async def on_tool_end():
        pass

    @on_start
    async def on_agent_action():
        pass

    @on_end
    async def on_agent_finish():
        pass

    @on_start
    async def on_retriever_start():
        pass

    @on_end
    async def on_retriever_end():
        pass
