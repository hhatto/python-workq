import uuid
import asyncio

from workq.job import LeasedJob
from workq.error import WorkqError, WorkqClientError, WorkqServerError, \
                        WorkqTimeout, WorkqJobIdNotFound


class WorkqProtocol(object):

    @staticmethod
    def check_response(msg):
        if msg[0] == ord('-'):
            if msg[1] == ord('C'):      # -CLIENT-ERROR
                raise WorkqClientError(msg.strip().split()[1])
            elif msg[1] == ord('S'):    # -SERVER-ERROR
                raise WorkqServerError(msg.strip().split()[1])
            elif msg[1] == ord('T'):    # -TIMED-OUT
                raise WorkqTimeout()
            elif msg[1] == ord('N'):    # -NOT-FOUND
                raise WorkqJobIdNotFound()
            raise WorkqError(msg.strip())
        elif msg[0] == ord('+'):
            pass
        else:
            raise WorkqError("invlaid workq protocol")
        # ok
        r = msg.strip()[4:]
        if len(r) == 0:
            return None
        return int(r)

    @staticmethod
    def parse_lease_body(msg):
        """parse lease body (line 2-3)

        <id> <name> <payload-size>
        <payload-bytes>
        """
        lines = msg.splitlines()
        _id, _name, _size = lines[0].strip().split()
        _size = int(_size)
        _payload = lines[1][:_size]
        return LeasedJob(_id.decode(), _name.decode(), _payload.decode())


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
    def lease(self, names, timeout=60000):
        proto_names = " ".join(names)
        msg = "lease %s %d\r\n" % (proto_names, timeout)
        # send request
        self._w.write(str.encode(msg))
        future = self._w.drain()
        yield from asyncio.wait_for(future, timeout=timeout/1000., loop=self.loop)

        # wait response
        future = self._r.readline()
        buf = yield from asyncio.wait_for(future, timeout=5, loop=self.loop)
        rnum = WorkqProtocol.check_response(buf)
        jobs = []
        for i in range(rnum):
            future = self._r.readline()
            buf = yield from asyncio.wait_for(future, timeout=5, loop=self.loop)
            future = self._r.readline()
            buf += yield from asyncio.wait_for(future, timeout=5, loop=self.loop)
            jobs.append(WorkqProtocol.parse_lease_body(buf))
        return jobs

    @asyncio.coroutine
    def add_job(self, job):
        # build request message for workq protocol
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

        # send request
        self._w.write(str.encode(msg))
        future = self._w.drain()
        yield from asyncio.wait_for(future, timeout=self._timeout, loop=self.loop)

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
    finally:
        loop.close()
