from typing import Dict, List, TypedDict
from httpx._types import CookieTypes


class GoogleTokens(TypedDict):
    accessToken: str
    refreshToken: str
    accessTokenExpiryDate: int
    REFRESH_IF_EXPIRES_IN_MINUTES: int
    projectId: str
    legacyProject: str


"""Google tokens sent from sepal to Solara as headers"""

SepalCookies = Dict[str, str]
"""Cookies sent from sepal to Solara for a given user"""


class SepalUser(TypedDict):
    id: int
    username: str
    googleTokens: GoogleTokens
    status: str
    roles: List[str]
    systemUser: bool
    admin: bool
    cookies: SepalCookies


class SepalHeaders(TypedDict):
    cookie: List[str]
    sepal_user: List[SepalUser]


"""Headers sent from sepal to Solara for a given user"""


GEEHeaders = TypedDict(
    "GEEHeaders", {"x-goog-user-project": str, "Authorization": str, "Username": str}
)


"""This will be the headers used for each request to the GEE API"""


class Credentials(TypedDict):
    client_id: str
    client_secret: str
    refresh_token: str
    grant_type: str


class GEECredentials(TypedDict):
    access_token: str
    access_token_expiry_date: int
    project_id: str
    sepal_user: str
