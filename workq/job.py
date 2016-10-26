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

    def to_proto(self):
        options = []
        if self.priority != 0:
            options.append("-priority=%d" % self.priority)
        if self.max_attempts != 0:
            options.append("-max-attempts=%d" % self.max_attempts)
        if self.max_fails != 0:
            options.append("-max-fails=%d" % self.max_fails)
        option_arg = "" if len(options) == 0 else " " + " ".join(options)
        msg = "add %s %s %d %d %d%s\r\n%s\r\n" % (
                self.id, self.name, self.ttr, self.ttl, len(self.payload), option_arg, self.payload)
        return msg


class ScheduledJob(object):

    def __init__(self, jobid, name, ttr, ttl, time, payload, priority=0, max_attempts=0, max_fails=0):
        self.id = jobid
        self.name = name
        self.ttr = ttr
        self.ttl = ttl
        self.time = time
        self.payload = payload
        self.priority = priority
        self.max_attempts = max_attempts
        self.max_fails = max_fails

    def to_proto(self):
        options = []
        if self.priority != 0:
            options.append("-priority=%d" % self.priority)
        if self.max_attempts != 0:
            options.append("-max-attempts=%d" % self.max_attempts)
        if self.max_fails != 0:
            options.append("-max-fails=%d" % self.max_fails)
        option_arg = "" if len(options) == 0 else " " + " ".join(options)
        t = self.time.isoformat("T").split('.')[0] + "Z"
        msg = "schedule %s %s %d %d %s %d%s\r\n%s\r\n" % (
                self.id, self.name, self.ttr, self.ttl, t, len(self.payload), option_arg, self.payload)
        return msg


class ForegroundJob(object):

    def __init__(self, jobid, name, ttr, timeout, payload, priority=0):
        self.id = jobid
        self.name = name
        self.ttr = ttr
        self.timeout = timeout
        self.payload = payload
        self.priority = priority

    def to_proto(self):
        options = []
        if self.priority != 0:
            options.append("-priority=%d" % self.priority)
        option_arg = "" if len(options) == 0 else " " + " ".join(options)
        msg = "run %s %s %d %d %d%s\r\n%s\r\n" % (
                self.id, self.name, self.ttr, self.timeout, len(self.payload), option_arg, self.payload)
        return msg


class LeasedJob(object):

    def __init__(self, jobid, name, payload):
        self.id = jobid
        self.name = name
        self.payload = payload
