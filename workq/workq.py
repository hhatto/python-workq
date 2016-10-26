import uuid
import asyncio

from workq.job import LeasedJob
from workq.error import WorkqError, WorkqClientError, WorkqServerError, \
                        WorkqTimeout, WorkqJobIdNotFound


class WorkqResult(object):

    def __init__(self, jobid):
        self.id = jobid
        self._success = 0   # 0: fail, 1: success
        self._result = ""

    @property
    def success(self):
        return self._success == 1

    @success.setter
    def success(self, v):
        self._success = v

    @property
    def result(self):
        return self._result

    @result.setter
    def result(self, v):
        self._result = v


class WorkqProtocol(object):

    @staticmethod
    def check_response(msg):
        if msg[0] == ord('-'):
            if msg[1] == ord('C'):      # -CLIENT-ERROR
                raise WorkqClientError(b" ".join(msg.strip().split()[1:]))
            elif msg[1] == ord('S'):    # -SERVER-ERROR
                raise WorkqServerError(b" ".join(msg.strip().split()[1:]))
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

    @staticmethod
    def parse_result_body(msg):
        """parse result body (line 2-3)

        <id> <success> <result-length>
        <result-block>
        """
        lines = msg.splitlines()
        _id, _success, _length = lines[0].strip().split()
        _success = int(_success)
        _length = int(_length)
        _payload = lines[1][:_length]
        result = WorkqResult(_id.decode())
        result.success = _success
        result.result = _payload.decode()
        return result


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
        yield from asyncio.wait_for(future, timeout=timeout, loop=self.loop)

    @asyncio.coroutine
    def connect(self):
        future = asyncio.open_connection(self.host, self.port, loop=self.loop)
        self._r, self._w = yield from asyncio.wait_for(future, timeout=self._timeout, loop=self.loop)

    @asyncio.coroutine
    def add(self, job):
        """add background job"""
        # build request message for add command, and send request
        msg = job.to_proto()
        yield from self.send(msg, self._timeout)

        # read reply
        future = self._r.readline()
        buf = yield from asyncio.wait_for(future, timeout=self._timeout, loop=self.loop)
        WorkqProtocol.check_response(buf)

    @asyncio.coroutine
    def schedule(self, job):
        # build request message for schedule command, and send request
        msg = job.to_proto()
        yield from self.send(msg, self._timeout)

        # read reply
        future = self._r.readline()
        buf = yield from asyncio.wait_for(future, timeout=self._timeout, loop=self.loop)
        WorkqProtocol.check_response(buf)

    @asyncio.coroutine
    def lease(self, names, timeout=60000):
        # build request message for lease command, and send request
        proto_names = " ".join(names)
        msg = "lease %s %d\r\n" % (proto_names, timeout)
        yield from self.send(msg, self._timeout)

        # recv response
        future = self._r.readline()
        buf = yield from asyncio.wait_for(future, timeout=timeout*1.3/1000., loop=self.loop)
        rnum = WorkqProtocol.check_response(buf)

        # recv response body
        jobs = []
        for i in range(rnum):
            future = self._r.readline()
            buf = yield from asyncio.wait_for(future, timeout=self._timeout, loop=self.loop)
            future = self._r.readline()
            buf += yield from asyncio.wait_for(future, timeout=self._timeout, loop=self.loop)
            jobs.append(WorkqProtocol.parse_lease_body(buf))
        return jobs

    @asyncio.coroutine
    def run(self, job):
        # build request message for schedule command, and send request
        msg = job.to_proto()
        yield from self.send(msg, self._timeout)

        # recv response
        future = self._r.readline()
        buf = yield from asyncio.wait_for(future, timeout=job.timeout*1.3/1000., loop=self.loop)
        yield from self.send(msg, self._timeout)

        # recv response
        future = self._r.readline()
        buf = yield from asyncio.wait_for(future, timeout=timeout*1.3/1000., loop=self.loop)
        rnum = WorkqProtocol.check_response(buf)

        # recv response body
        jobs = []
        for i in range(rnum):
            future = self._r.readline()
            buf = yield from asyncio.wait_for(future, timeout=self._timeout, loop=self.loop)
            future = self._r.readline()
            buf += yield from asyncio.wait_for(future, timeout=self._timeout, loop=self.loop)
            jobs.append(WorkqProtocol.parse_lease_body(buf))
        return jobs

    @asyncio.coroutine
    def result(self, job_id, timeout=60000):
        # build request message for complete command, and send request
        msg = "result %s %d\r\n" % (job_id, timeout)
        yield from self.send(msg, self._timeout)

        # check reply
        future = self._r.readline()
        buf = yield from asyncio.wait_for(future, timeout=timeout/1000., loop=self.loop)
        rnum = WorkqProtocol.check_response(buf)

        # recv response body
        results = []
        for i in range(rnum):
            future = self._r.readline()
            buf = yield from asyncio.wait_for(future, timeout=self._timeout, loop=self.loop)
            future = self._r.readline()
            buf += yield from asyncio.wait_for(future, timeout=self._timeout, loop=self.loop)
            results.append(WorkqProtocol.parse_result_body(buf))
        return results

    @asyncio.coroutine
    def complete(self, job_id, result):
        # build request message for complete command, and send request
        msg = "complete %s %d\r\n%s\r\n" % (job_id, len(result), result)
        yield from self.send(msg, self._timeout)

        # check reply
        future = self._r.readline()
        buf = yield from asyncio.wait_for(future, timeout=self._timeout, loop=self.loop)
        WorkqProtocol.check_response(buf)

    @asyncio.coroutine
    def fail(self, job_id, result):
        # build request message for fail command, and send request
        msg = "fail %s %d\r\n%s\r\n" % (job_id, len(result), result)
        yield from self.send(msg, self._timeout)

        # check reply
        future = self._r.readline()
        buf = yield from asyncio.wait_for(future, timeout=self._timeout, loop=self.loop)
        WorkqProtocol.check_response(buf)

    @asyncio.coroutine
    def delete(self, job_id):
        # build delete command message, and send request
        msg = "delete %s\r\n" % (job_id)
        yield from self.send(msg, self._timeout)

        # read reply
        future = self._r.readline()
        buf = yield from asyncio.wait_for(future, timeout=self._timeout, loop=self.loop)
        WorkqProtocol.check_response(buf)
