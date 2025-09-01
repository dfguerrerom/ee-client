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

    def __init__(self, error):
        if isinstance(error, str):
            self.message = error
            self.code = -1
            self.status = "UNDEFINED"
            self.details = None
        else:
            self.message = error.get("message", "EEClient responded with an error")
            self.code = error.get("code", -1)
            self.status = error.get("status", "UNDEFINED")
            self.details = error.get("details")
        super().__init__(self.message)


class CredentialsFileNotFoundError(EEClientError):
    """Raised when local credentials file is not found."""

    def __init__(self, file_path: str):
        self.file_path = file_path
        error = {
            "code": 404,
            "message": (
                "There was an error when trying to authenticate with Google "
                "Earth Engine. Make sure your GEE account is properly "
                "connected to SEPAL. For more information, please visit "
                "<a href='https://docs.sepal.io/en/latest/setup/gee.html' "
                "target='_blank'> the documentation </a>."
            ),
            "status": "CREDENTIALS_NOT_FOUND",
            "details": f"Credentials file not found at {file_path}",
        }
        super().__init__(error)


class CredentialsFileUnknownError(EEClientError):
    """Raised when unknown error occurred while accessing credentials file."""

    def __init__(self):
        error = {
            "code": 500,
            "message": (
                "There was an error when trying to authenticate with Google "
                "Earth Engine. Please re-authenticate your GEE account with "
                "SEPAL. For more information, visit "
                "<a href='https://docs.sepal.io/en/latest/setup/gee.html' "
                "target='_blank'> the documentation </a>."
            ),
            "status": "CREDENTIALS_ERROR",
            "details": "Unknown error occurred while accessing credentials file",
        }
        super().__init__(error)


class SepalCredentialsUnavailableError(EEClientError):
    """Raised when SEPAL API cannot provide credentials (e.g., 500 error)."""

    def __init__(self, status_code: int | None = None):
        self.status_code = status_code
        error = {
            "code": status_code or 500,
            "message": (
                "There was an error when trying to authenticate with Google "
                "Earth Engine. Make sure your GEE account is properly "
                "connected to SEPAL. Please visit "
                "<a href='https://docs.sepal.io/en/latest/setup/gee.html' "
                "target='_blank'> the documentation </a> for more information."
            ),
            "status": "SEPAL_CREDENTIALS_UNAVAILABLE",
            "details": (
                f"SEPAL API returned error {status_code}"
                if status_code
                else "SEPAL API is unable to provide credentials"
            ),
        }
        super().__init__(error)


# {'code': 401, 'message': 'Request had invalid authentication credentials.
# Expected OAuth 2 access token, login cookie or other valid authentication
# credential. See https://developers.google.com/identity/sign-in/web/devconsole-project.',
# 'status': 'UNAUTHENTICATED'}
# Exception in _run_task: Request had invalid authentication credentials.
# Expected OAuth 2 access token, login cookie or other valid authentication
# credential. See https://developers.google.com/identity/sign-in/web/devconsole-project.
# when that error happens, I need to re-set the credentials
