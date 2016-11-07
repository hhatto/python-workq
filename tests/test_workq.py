import unittest
import uuid
import asyncio
from datetime import datetime, timedelta
from workq.workq import WorkqClient
from workq.error import WorkqTimeout, WorkqJobIdNotFound
from workq.job import BackgroundJob, ScheduledJob, ForegroundJob


class TestWorkq(unittest.TestCase):

    def test_add(self):
        loop = asyncio.new_event_loop()
        client = WorkqClient('127.0.0.1', 9922, loop)
        jobid = uuid.uuid4()
        job = BackgroundJob(jobid, "ping1", 5000, 60000, "hello bg job")
        try:
            loop.run_until_complete(client.connect())
            loop.run_until_complete(client.add(job))
            leased_job = loop.run_until_complete(client.lease(("ping1", ), 10000))
            self.assertEqual(len(leased_job), 1)
            self.assertEqual(leased_job[0].id, "%s" % jobid)
            self.assertEqual(leased_job[0].name, "ping1")
        finally:
            loop.close()

    def test_schedule(self):
        loop = asyncio.new_event_loop()
        client = WorkqClient('127.0.0.1', 9922, loop)
        jobid = uuid.uuid4()
        t = datetime.utcnow() + timedelta(seconds=1)
        job = ScheduledJob(jobid, "ping1", 5000, 60000, t, "hello bg job")
        try:
            loop.run_until_complete(client.connect())
            loop.run_until_complete(client.add(job))
            leased_job = loop.run_until_complete(client.lease(("ping1", ), 10000))
            self.assertEqual(len(leased_job), 1)
            self.assertEqual(leased_job[0].id, "%s" % jobid)
            self.assertEqual(leased_job[0].name, "ping1")
        finally:
            loop.close()

    def test_run(self):
        self.skipTest("broken")
        loop = asyncio.new_event_loop()
        client = WorkqClient('127.0.0.1', 9922, loop)
        jobid = uuid.uuid4()
        job = ForegroundJob(jobid, "fg1", 5000, 60000, "hello fg job")

        @asyncio.coroutine
        def parallel(client, loop):
            run_task = asyncio.ensure_future(client.run(job), loop=loop)
            lease_task = asyncio.ensure_future(client.lease(("fg1", ), 10000), loop=loop)
            tasks = [run_task, lease_task]
            results = yield from asyncio.gather(*tasks, loop=loop)
            return results
        try:
            loop.run_until_complete(client.connect())
            loop.run_until_complete(parallel(client, loop))
        finally:
            loop.close()

    def test_lease_timeout(self):
        loop = asyncio.new_event_loop()
        client = WorkqClient('127.0.0.1', 9922, loop)
        try:
            loop.run_until_complete(client.connect())
            with self.assertRaises(WorkqTimeout):
                leased_job = loop.run_until_complete(client.lease(("ping1", ), 100))
                print(leased_job)
        finally:
            loop.close()

    def test_complete(self):
        loop = asyncio.new_event_loop()
        client = WorkqClient('127.0.0.1', 9922, loop)
        jobid = uuid.uuid4()
        job = BackgroundJob(jobid, "test.complete1", 5000, 60000, "hello bg job")
        try:
            loop.run_until_complete(client.connect())
            loop.run_until_complete(client.add(job))
            leased_job = loop.run_until_complete(client.lease(("test.complete1", ), 10000))
            self.assertEqual(len(leased_job), 1)
            ljob = leased_job[0]
            loop.run_until_complete(client.complete(ljob.id, "ok"))
        finally:
            loop.close()

    def test_fail(self):
        loop = asyncio.new_event_loop()
        client = WorkqClient('127.0.0.1', 9922, loop)
        jobid = uuid.uuid4()
        job = BackgroundJob(jobid, "test.complete1", 5000, 60000, "hello bg job")
        try:
            loop.run_until_complete(client.connect())
            loop.run_until_complete(client.add(job))
            leased_job = loop.run_until_complete(client.lease(("test.complete1", ), 10000))
            self.assertEqual(len(leased_job), 1)
            ljob = leased_job[0]
            loop.run_until_complete(client.fail(ljob.id, "ng"))
        finally:
            loop.close()

    def test_delete(self):
        loop = asyncio.new_event_loop()
        client = WorkqClient('127.0.0.1', 9922, loop)
        jobid = uuid.uuid4()
        job = BackgroundJob(jobid, "test.complete1", 5000, 60000, "hello bg job")
        try:
            loop.run_until_complete(client.connect())
            loop.run_until_complete(client.add(job))
            loop.run_until_complete(client.delete(job.id))
        finally:
            loop.close()

    def test_delete_with_error(self):
        loop = asyncio.new_event_loop()
        client = WorkqClient('127.0.0.1', 9922, loop)
        jobid = uuid.uuid4()
        try:
            loop.run_until_complete(client.connect())
            with self.assertRaises(WorkqJobIdNotFound):
                loop.run_until_complete(client.delete(jobid))
        finally:
            loop.close()

if __name__ == '__main__':
    unittest.main()
