from typing import Dict, List, TypedDict
from httpx._types import CookieTypes, HeaderTypes
from typing import Union


class MapTileOptions(TypedDict):
    """
    MapTileOptions defines the configuration for map tile generation.

    Keys:
        min (str or List[str]): Comma-separated numbers representing the values
            to map onto 00. It can be a string of comma-separated numbers
            (e.g., "1,2,3") or a list of strings. (e.g., ["1", "2", "3"]).
        max (str or List[str]): Comma-separated numbers representing the values
            to map onto FF. It can be a string of comma-separated numbers or
            a list of strings.
        gain (str or List[str]): Comma-separated numbers representing the gain
            to map onto 00-FF. It can be a string of comma-separated numbers or
            a list of strings.
        bias (str or List[str]): Comma-separated numbers representing the
            offset to map onto 00-FF. It can be a string of comma-separated
            numbers or a list of strings.
        gamma (str or List[str]): Comma-separated numbers representing the
            gamma correction factor. It can be a string of comma-separated
            numbers or a list of strings.
        palette (str): A string of comma-separated CSS-style color strings
            (single-band previews only).For example, 'FF0000,000000'.
        format (str): The desired map tile format.
    """

    min: Union[str, List[str]]
    max: Union[str, List[str]]
    gain: Union[str, List[str]]
    bias: Union[str, List[str]]
    gamma: Union[str, List[str]]
    palette: str
    format: str


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


class SepalHeaders(TypedDict):
    cookie: List[str]
    sepal_user: List[SepalUser]


"""Headers sent from sepal to Solara for a given user"""


GEEHeaders = TypedDict(
    "GEEHeaders",
    {
        "x-goog-user-project": str,
        "Authorization": str,
        "Username": str,
    },
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
