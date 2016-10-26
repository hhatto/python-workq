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

    def __init__(self, host='localhost', port=9922, loop=None):
        self.host = host
        self.port = port
        self.loop = loop or asyncio.get_event_loop()
        self._timeout = 3

    @asyncio.coroutine
    def send(self, msg, timeout):
        self._w.write(str.encode(msg))
        future = self._w.drain()
        yield from asyncio.wait_for(future, timeout=timeout/1000., loop=self.loop)

    @asyncio.coroutine
    def connect(self):
        future = asyncio.open_connection(self.host, self.port, loop=self.loop)
        self._r, self._w = yield from asyncio.wait_for(future, timeout=self._timeout, loop=self.loop)

    @asyncio.coroutine
    def lease(self, names, timeout=60000):
        # build request message for lease command
        proto_names = " ".join(names)
        msg = "lease %s %d\r\n" % (proto_names, timeout)

        # send request
        yield from self.send(msg, self._timeout)

        # recv response
        future = self._r.readline()
        buf = yield from asyncio.wait_for(future, timeout=timeout*1.3/1000., loop=self.loop)
        rnum = WorkqProtocol.check_response(buf)

        # recv response body
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
        """add background job"""
        # build request message for add command
        msg = job.to_proto()

        # send request
        yield from self.send(msg, self._timeout)

        # read reply
        future = self._r.readline()
        buf = yield from asyncio.wait_for(future, timeout=self._timeout, loop=self.loop)
        WorkqProtocol.check_response(buf)
