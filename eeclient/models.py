import json
from typing import Optional, Union, List, Dict
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic.alias_generators import to_camel

from eeclient.exceptions import EEClientError


class MapTileOptions(BaseModel):
    """
    MapTileOptions defines the configuration for map tile generation.

    Attributes:
        min (Union[str, List[str]]): Comma-separated numbers representing the values to map onto 00.
        max (Union[str, List[str]]): Comma-separated numbers representing the values to map onto FF.
        gain (Union[str, List[str]]): Comma-separated numbers representing the gain to map onto 00-FF.
        bias (Union[str, List[str]]): Comma-separated numbers representing the offset to map onto 00-FF.
        gamma (Union[str, List[str]]): Comma-separated numbers representing the gamma correction factor.
        palette (str): A string of comma-separated CSS-style color strings (single-band previews only).
        format (str): The desired map tile format.
    """

    min: Union[str, List[str]]
    max: Union[str, List[str]]
    gain: Union[str, List[str]]
    bias: Union[str, List[str]]
    gamma: Union[str, List[str]]
    palette: str
    format: str


# A type alias for cookies
SepalCookies = Dict[str, str]


class GoogleTokens(BaseModel):
    access_token: str
    refresh_token: Optional[str] = None
    access_token_expiry_date: int
    refresh_if_expires_in_minutes: Optional[int] = None
    project_id: str
    legacy_project: Optional[bool] = None

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
    )

    @model_validator(mode="after")
    def check_google_tokens(self) -> "GoogleTokens":
        # Ensure that the project_id in google_tokens is provided
        if not self.project_id:
            raise EEClientError(
                "No project ID found in the user data. Please authenticate select a project."
            )
        return self

    @model_validator(mode="before")
    def parse_if_string(cls, v):
        """
        If the input is a JSON string, parse it into a dict.
        """
        if isinstance(v, str):
            try:
                return json.loads(v)
            except json.JSONDecodeError as e:
                raise ValueError("Invalid JSON for GoogleTokens") from e
        return v


class SepalUser(BaseModel):
    username: str
    id: Optional[int] = None
    google_tokens: Optional[GoogleTokens] = None
    status: Optional[str] = None
    roles: Optional[List[str]] = None
    system_user: Optional[bool] = None

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
    )


class SepalHeaders(BaseModel):
    cookies: SepalCookies = Field(..., alias="cookie")
    sepal_user: SepalUser = Field(..., alias="sepal-user")

    @field_validator("cookies", mode="before")
    def parse_cookies(cls, v):
        """
        Accepts a list of cookie strings or a single cookie string and converts them into a dictionary.
        Example input: ['SEPAL-SESSIONID=s:token; OTHERCOOKIE=foo;']
        """
        cookies = {}
        if isinstance(v, list):
            for cookie_str in v:
                if isinstance(cookie_str, str):
                    for cookie_pair in cookie_str.split(";"):
                        cookie_pair = cookie_pair.strip()
                        if cookie_pair:
                            key, sep, value = cookie_pair.partition("=")
                            if sep:
                                cookies[key] = value
        elif isinstance(v, str):
            for cookie_pair in v.split(";"):
                cookie_pair = cookie_pair.strip()
                if cookie_pair:
                    key, sep, value = cookie_pair.partition("=")
                    if sep:
                        cookies[key] = value
        elif isinstance(v, dict):
            return v
        return cookies

    @field_validator("sepal_user", mode="before")
    def parse_sepal_user(cls, v):
        """
        Accepts the sepal-user field which may come as:
          - A list with a single element.
          - A JSON string representing the user.
          - A dict representing the user.
        Automatically extracts the only element if provided as a list.
        """
        if isinstance(v, list):
            if len(v) != 1:
                raise ValueError("sepal-user should contain exactly one element")
            v = v[0]
        if isinstance(v, str):
            try:
                return json.loads(v)
            except json.JSONDecodeError as e:
                raise ValueError("Invalid JSON string in sepal-user field") from e
        return v

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
    )


class GEEHeaders(BaseModel):
    x_goog_user_project: str = Field(..., alias="x-goog-user-project")
    authorization: str = Field(..., alias="Authorization")
    username: str = Field(..., alias="Username")

    model_config = ConfigDict(
        populate_by_name=True,
    )


class Credentials(BaseModel):
    client_id: str
    client_secret: str
    refresh_token: str
    grant_type: str


class GEECredentials(BaseModel):
    access_token: str
    access_token_expiry_date: int
    project_id: str
    sepal_user: str
