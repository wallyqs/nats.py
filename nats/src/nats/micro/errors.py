# Copyright 2021-2024 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Errors raised by the services framework, mirroring nats.go micro's
``Err*`` values and ``NATSError``.

Errors that replace a ``ValueError`` raised by earlier releases also
subclass ``ValueError``, so existing ``except ValueError`` clauses keep
working.
"""

from __future__ import annotations

import json
from typing import Optional

import nats.errors


class MicroError(nats.errors.Error):
    """Base class of the errors raised by ``nats.micro``."""

    default_message = ""

    def __init__(self, message: Optional[str] = None) -> None:
        super().__init__(self.default_message if message is None else message)

    def __str__(self) -> str:
        return str(self.args[0]) if self.args else ""


class ConfigValidationError(MicroError, ValueError):
    """A service, group or endpoint configuration is invalid (``ErrConfigValidation``)."""

    default_message = "validation"


class VerbNotSupportedError(MicroError, ValueError):
    """A verb other than PING, STATS or INFO was given (``ErrVerbNotSupported``)."""

    default_message = "unsupported verb"


class ServiceNameRequiredError(MicroError, ValueError):
    """A control subject for an instance id was requested without a service
    name (``ErrServiceNameRequired``)."""

    default_message = "service name is required to generate ID control subject"


class RespondError(MicroError, ValueError):
    """A response could not be sent (``ErrRespond``)."""

    default_message = "NATS error when sending response"


class MarshalResponseError(MicroError, ValueError):
    """A ``respond_json`` value could not be marshalled to JSON (``ErrMarshalResponse``)."""

    default_message = "marshaling response"


class ArgRequiredError(MicroError, ValueError):
    """An error response was sent without a code or description (``ErrArgRequired``)."""

    default_message = "argument required"


class NATSError(MicroError):
    """An asynchronous error reported for one of a service's subscriptions,
    as passed to ``ServiceConfig.error_handler``.

    ``subject`` is the subject of the subscription so that the error can be
    linked to an endpoint, ``description`` the error's text. Two
    ``NATSError`` values are equal when their subject and description are,
    as with nats.go's ``NATSError.Is``; ``unwrap()`` returns the reported
    error.
    """

    def __init__(self, subject: str, description: str, error: Optional[BaseException] = None) -> None:
        self.subject = subject
        self.description = description
        self.error = error
        super().__init__(f"{json.dumps(subject)}: {description}")
        if error is not None:
            self.__cause__ = error

    def unwrap(self) -> Optional[BaseException]:
        """The error that was reported, if any."""
        return self.error

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, NATSError):
            return NotImplemented
        return self.subject == other.subject and self.description == other.description

    def __hash__(self) -> int:
        return hash((self.subject, self.description))

    def __repr__(self) -> str:
        return f"NATSError(subject={self.subject!r}, description={self.description!r})"


__all__ = [
    "ArgRequiredError",
    "ConfigValidationError",
    "MarshalResponseError",
    "MicroError",
    "NATSError",
    "RespondError",
    "ServiceNameRequiredError",
    "VerbNotSupportedError",
]
