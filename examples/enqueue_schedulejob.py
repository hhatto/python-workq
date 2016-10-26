import asyncio
import time
import uuid
from datetime import datetime, timedelta
from workq.workq import WorkqClient
from workq.job import ScheduledJob


def main():
    loop = asyncio.new_event_loop()
    client = WorkqClient('127.0.0.1', 9922, loop)
    jobid = uuid.uuid4()
    t = datetime.utcnow() + timedelta(seconds=1)
    job = ScheduledJob(jobid, "ping1", 5000, 60000, t, "hello fg job")
    try:
        loop.run_until_complete(client.connect())
        loop.run_until_complete(client.schedule(job))
        time.sleep(2)
        results = loop.run_until_complete(client.result(jobid, 10000))
        for result in results:
            print(result.success, result.result)
    finally:
        loop.close()


if __name__ == '__main__':
    main()
