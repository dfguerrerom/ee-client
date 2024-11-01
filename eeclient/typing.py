from typing import List, TypedDict


class GoogleTokens(TypedDict):
    accessToken: str
    refreshToken: str
    accessTokenExpiryDate: int
    REFRESH_IF_EXPIRES_IN_MINUTES: int
    projectId: str
    legacyProject: str


"""Google tokens sent from sepal to Solara as headers"""


class SepalHeaders(TypedDict):
    id: int
    username: str
    googleTokens: GoogleTokens
    status: str
    roles: List[str]
    systemUser: bool
    admin: bool


"""Headers sent from sepal to Solara for a given user"""


GEEHeaders = TypedDict("GEEHeaders", {"x-goog-user-project": str, "Authorization": str})


"""This will be the headers used for each request to the GEE API"""


class Credentials(TypedDict):
    client_id: str
    client_secret: str
    refresh_token: str
    grant_type: str
