import unittest
import uuid
import asyncio
from workq.workq import WorkqClient
from workq.error import WorkqTimeout
from workq.job import BackgroundJob


class TestWorkq(unittest.TestCase):

    def test_smoke(self):
        loop = asyncio.new_event_loop()
        client = WorkqClient('127.0.0.1', 9922, loop)
        jobid = uuid.uuid4()
        job = BackgroundJob(jobid, "ping1", 5000, 60000, "hello bg job")
        try:
            loop.run_until_complete(client.connect())
            loop.run_until_complete(client.add_job(job))
            leased_job = loop.run_until_complete(client.lease(("ping1", ), 10000))
            self.assertEqual(len(leased_job), 1)
            self.assertEqual(leased_job[0].id, "%s" % jobid)
            self.assertEqual(leased_job[0].name, "ping1")
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

if __name__ == '__main__':
    unittest.main()
