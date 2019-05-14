import inspect
from blazingdb.protocol import Client, AsyncClient, UnixSocketConnection

def bind_request(create_request, handle_response):

    def do_request(self, *args, **kwargs):
        request_buffer = create_request(self, *args, **kwargs)
        connection = UnixSocketConnection(self._path)
        return Client(connection).send(request_buffer)

    async def do_request_async(self, *args, **kwargs):
        request_buffer = create_request(self, *args, **kwargs)
        connection = UnixSocketConnection(self._path)
        return await AsyncClient(connection).send(request_buffer)

    async def handle_response_async(self, response_coroutine):
        result = handle_response(self, await response_coroutine)
        while is_awaitable(result):
            result = await result
        return result

    def request(self, *args, **kwargs):
        if is_caller_async():
            return handle_response_async(self, do_request_async(self, *args, **kwargs))
        return handle_response(self, do_request(self, *args, **kwargs))

    return request


def is_caller_async():
    """Figure out who's calling."""
    # Get the calling frame
    caller = inspect.currentframe().f_back.f_back
    original_caller = caller
    while caller:
        # Pull the function name from FrameInfo
        func_name = inspect.getframeinfo(caller)[2]
        # Get the function object
        f = caller.f_locals.get(func_name, caller.f_globals.get(func_name))
        # If there's any indication that the function object is a 
        # coroutine, return True. inspect.iscoroutinefunction() should
        # be all we need, the rest are here to illustrate.
        if is_async_thing(f):
            return True
        caller = caller.f_back
    return False


def is_async_thing(thing):
    return is_async_func(thing) or is_awaitable(thing)


def is_async_func(func):
    if inspect.iscoroutinefunction(func):
        return True
    if inspect.isgeneratorfunction(func):
        return True
    if inspect.isasyncgenfunction(func):
        return True
    return False


def is_awaitable(thing):
    if inspect.iscoroutine(thing):
        return True
    if inspect.isawaitable(thing):
        return True
    if inspect.isasyncgen(thing):
        return True
    return False
