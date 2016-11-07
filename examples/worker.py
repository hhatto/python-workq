import asyncio
from workq.error import WorkqTimeout
from workq.workq import WorkqClient


def main():
    loop = asyncio.new_event_loop()
    client = WorkqClient('127.0.0.1', 9922, loop)
    loop.run_until_complete(client.connect())
    while True:
        try:
            jobs = loop.run_until_complete(client.lease(('ping1', ), 10000))
        except WorkqTimeout:
            continue
        for job in jobs:
            print("job-id: %s, %s, %s" % (job.id, job.name, job.payload))
            loop.run_until_complete(client.complete(job.id, 'ok'))
    loop.close()

if __name__ == '__main__':
    main()
