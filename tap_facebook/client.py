"""REST client handling, including facebookStream base class."""

from __future__ import annotations

import abc
import json
import typing as t
from http import HTTPStatus
from urllib.parse import urlparse

import pendulum
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context


class FacebookStream(RESTStream):
    """facebook stream class."""

    # add account id in the url
    # path and fields will be added to this url in streams.pys

    @property
    def url_base(self) -> str:
        version: str = self.config["api_version"]
        account_id: str = self.config["account_id"]
        return f"https://graph.facebook.com/{version}/act_{account_id}"

    records_jsonpath = "$.data[*]"  # Or override `parse_response`.
    next_page_token_jsonpath = "$.paging.cursors.after"  # noqa: S105

    tolerated_http_errors: list[int] = []  # noqa: RUF012

    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return BearerTokenAuthenticator.create_for_stream(
            self,
            token=self.config["access_token"],
        )

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: t.Any | None,  # noqa: ARG002, ANN401
    ) -> t.Any | None:  # noqa: ANN401
        """Return a token for identifying next page or None if no more pages.

        Args:
            response: The HTTP ``requests.Response`` object.
            previous_token: The previous page token value.

        Returns:
            The next pagination token.
        """
        if not self.next_page_token_jsonpath:
            return response.headers.get("X-Next-Page", None)

        all_matches = extract_jsonpath(
            self.next_page_token_jsonpath,
            response.json(),
        )
        return next(iter(all_matches), None)

    def get_url_params(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {"limit": 25}
        if next_page_token is not None:
            params["after"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key

        return params

    def validate_response(self, response: requests.Response) -> None:
        """Validate the API response, skipping gracefully on certain permission errors."""
        full_path = response.request.url  # use the original request URL for better context

        # Handle tolerated HTTP errors
        if response.status_code in self.tolerated_http_errors:
            self.logger.info(f"Tolerated {response.status_code} for {full_path}")
            return

        # Handle client errors (4xx)
        if HTTPStatus.BAD_REQUEST <= response.status_code < HTTPStatus.INTERNAL_SERVER_ERROR:
            msg = f"{response.status_code} Client Error: {response.content!s} for {full_path}"

            try:
                error_data = response.json().get("error", {})
                error_type = error_data.get("type")
                error_code = error_data.get("code")
                error_message = error_data.get("message")

                # If this is an OAuthException (missing permissions), skip gracefully
                if error_type == "OAuthException" and error_code == 200:
                    self.logger.warning(
                        f"Skipping stream {self.name} due to missing permissions: {error_message} "
                        f"(status {response.status_code}, path: {full_path})"
                    )
                    # Gracefully return without raising an error
                    # This effectively skips this record/stream
                    return
            except Exception as parse_exc:
                self.logger.warning(
                    f"Failed to parse error details from response JSON: {parse_exc}"
                )

            # If it's not a known permission error, raise a fatal error
            raise FatalAPIError(msg)

        # Handle server errors (5xx)
        if response.status_code >= HTTPStatus.INTERNAL_SERVER_ERROR:
            msg = f"{response.status_code} Server Error: {response.content!s} for {full_path}"
            raise RetriableAPIError(msg)


    def backoff_max_tries(self) -> int:
        """The number of attempts before giving up when retrying requests.

        Setting to None will retry indefinitely.

        Returns:
            int: limit
        """
        return 20


class IncrementalFacebookStream(FacebookStream, metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def filter_entity(self) -> str:
        """The entity to filter on."""

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {"limit": 25}
        if next_page_token is not None:
            params["after"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
            ts = pendulum.parse(self.get_starting_replication_key_value(context))  # type: ignore[arg-type]
            params["filtering"] = json.dumps(
                [
                    {
                        "field": f"{self.filter_entity}.{self.replication_key}",
                        "operator": "GREATER_THAN",
                        "value": int(ts.timestamp()),  # type: ignore[union-attr]
                    },
                ],
            )

        return params
