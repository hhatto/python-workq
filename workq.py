import uuid
import asyncio


class WorkqError(Exception):
    """Workq Base Error"""
    def __init__(slef, msg):
        self.msg = msg


class WorkqProtocol(object):

    @staticmethod
    def check_response(msg):
        if msg[0] == '-':
            raise WorkqError(msg.strip())
        elif msg == b'+OK\r\n':
            pass
        else:
            raise WorkqError("invlaid workq protocol")


class BackgroundJob(object):

    def __init__(self, jobid, name, ttr, ttl, payload, priority=0, max_attempts=0, max_fails=0):
        self.id = jobid
        self.name = name
        self.ttr = ttr
        self.ttl = ttl
        self.payload = payload
        self.priority = priority
        self.max_attempts = max_attempts
        self.max_fails = max_fails


class WorkqClient(object):

    def __init__(self, host, port, loop):
        self.host = host
        self.port = port
        self.loop = loop
        self._timeout = 3

    @asyncio.coroutine
    def connect(self):
        future = asyncio.open_connection(self.host, self.port, loop=self.loop)
        self._r, self._w = yield from asyncio.wait_for(future, timeout=self._timeout, loop=self.loop)

    @asyncio.coroutine
    def lease(self):
        print("lease")
        while True:
            future = self._r.readline()
            try:
                buf = yield from asyncio.wait_for(future, timeout=5, loop=self.loop)
            except asyncio.TimeoutError:
                print("lease timeout")
                return
            print("lease:", buf, future)

    @asyncio.coroutine
    def add_job(self, job):
        print("add_job")
        options = []
        if job.priority != 0:
            options.append("-priority=%d" % job.priority)
        if job.max_attempts != 0:
            options.append("-max-attempts=%d" % job.max_attempts)
        if job.max_fails != 0:
            options.append("-max-fails=%d" % job.max_fails)
        option_arg = "" if len(options) == 0 else " " + " ".join(options)
        msg = "add %s %s %d %d %d%s\r\n%s\r\n" % (
                job.id, job.name, job.ttr, job.ttl, len(job.payload), option_arg, job.payload)
        self._w.write(str.encode(msg))
        future = self._w.drain()
        try:
            yield from asyncio.wait_for(future, timeout=self._timeout, loop=self.loop)
        except asyncio.TimeoutError:
            print("add_job request timeout")
            return
        # read reply
        future = self._r.readline()
        buf = yield from asyncio.wait_for(future, timeout=self._timeout, loop=self.loop)
        WorkqProtocol.check_response(buf)


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    client = WorkqClient('127.0.0.1', 9922, loop)
    job = BackgroundJob(uuid.uuid4(), "ping1", 5000, 60000, "hello bg job")
    try:
        loop.run_until_complete(client.connect())
        loop.run_until_complete(client.add_job(job))
        loop.run_until_complete(client.lease())
        loop.run_until_complete(client.lease())
    finally:
        loop.close()
