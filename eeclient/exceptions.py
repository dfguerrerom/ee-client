from ee.ee_exception import EEException


class EERestException(EEException):
    def __init__(self, error):
        self.message = error.get("message", "EE responded with an error")
        super().__init__(self.message)
        self.code = error.get("code", -1)
        self.status = error.get("status", "UNDEFINED")
        self.details = error.get("details")


class EEClientError(Exception):
    """Custom exception class for EEClient errors."""

    pass


# {'code': 401, 'message': 'Request had invalid authentication credentials. Expected OAuth 2 access token, login cookie or other valid authentication credential. See https://developers.google.com/identity/sign-in/web/devconsole-project.', 'status': 'UNAUTHENTICATED'}
# Exception in _run_task: Request had invalid authentication credentials. Expected OAuth 2 access token, login cookie or other valid authentication credential. See https://developers.google.com/identity/sign-in/web/devconsole-project.
# when that error happens, I need to re-set the credentials
