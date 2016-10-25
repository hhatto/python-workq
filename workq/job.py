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


class LeasedJob(object):

    def __init__(self, jobid, name, payload):
        self.id = jobid
        self.name = name
        self.payload = payload
