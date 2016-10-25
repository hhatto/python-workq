import unittest
import uuid
import asyncio
from workq.workq import WorkqClient
from workq.job import BackgroundJob


class TestWorkq(unittest.TestCase):

    def test_smoke(self):
        loop = asyncio.new_event_loop()
        client = WorkqClient('127.0.0.1', 9922, loop)
        job = BackgroundJob(uuid.uuid4(), "ping1", 5000, 60000, "hello bg job")
        try:
            loop.run_until_complete(client.connect())
            loop.run_until_complete(client.add_job(job))
            leased_job = loop.run_until_complete(client.lease(("ping1", ), 10000))
            print(leased_job)
        finally:
            loop.close()

if __name__ == '__main__':
    unittest.main()
