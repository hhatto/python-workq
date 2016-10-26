python-workq
============

Python Client for Workq_ with asyncio_ .

.. _Workq: https://github.com/iamduo/workq
.. _asyncio: https://docs.python.org/dev/library/asyncio.html

Requirements
------------
* Python3.5+


Usage
-----

Enqueue Foreground Job
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    import asyncio
    import uuid
    from workq.workq import WorkqClient
    from workq.job import ForegroundJob


    def main():
        loop = asyncio.new_event_loop()
        client = WorkqClient('127.0.0.1', 9922, loop)
        jobid = uuid.uuid4()
        job = ForegroundJob(jobid, "ping1", 5000, 60000, "hello fg job")
        try:
            loop.run_until_complete(client.connect())
            results = loop.run_until_complete(client.run(job))
        finally:
            loop.close()
        for result in results:
            print("job: %s %s %s" % (result.id, result.name, result.payload))

    if __name__ == '__main__':
        main()


TODO
----
* [ ] inspect command
