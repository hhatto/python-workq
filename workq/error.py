
class WorkqError(Exception):
    """Workq Base Error"""
    def __init__(self, msg=""):
        self.msg = msg


class WorkqClientError(WorkqError):
    pass


class WorkqServerError(WorkqError):
    pass


class WorkqTimeout(WorkqError):
    pass


class WorkqJobIdNotFound(WorkqError):
    pass
