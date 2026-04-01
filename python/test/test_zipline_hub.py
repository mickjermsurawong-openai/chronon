#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import os
from datetime import date, timedelta
from unittest.mock import Mock, patch, MagicMock

import pytest
import requests

from ai.chronon.repo.zipline_hub import ZiplineHub


class TestZiplineHub:
    """Test cases for ZiplineHub class."""

    def test_base_url_strips_trailing_slash(self):
        """Test that base_url strips trailing slashes."""
        hub_with_slash = ZiplineHub("http://example.com/")
        assert hub_with_slash.base_url == "http://example.com"

        hub_with_multiple_slashes = ZiplineHub("http://example.com///")
        assert hub_with_multiple_slashes.base_url == "http://example.com"

        hub_without_slash = ZiplineHub("http://example.com")
        assert hub_without_slash.base_url == "http://example.com"

    @patch("requests.post")
    def test_call_workflow_start_api_with_default_dates(self, mock_post):
        """Test workflow start API call with default dates."""
        mock_response = Mock()
        mock_response.json.return_value = {"workflowId": "456"}
        mock_post.return_value = mock_response

        hub = ZiplineHub("http://example.com")
        result = hub.call_workflow_start_api(
            "test_conf", "daily", "main", "user1", "hash123"
        )

        expected_start = str(date.today() - timedelta(days=14))
        expected_end = str(date.today())

        assert result == {"workflowId": "456"}
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        assert call_kwargs[0][0] == "http://example.com/workflow/v2/start"
        assert call_kwargs[1]["json"] == {
            "confName": "test_conf",
            "confHash": "hash123",
            "mode": "daily",
            "branch": "main",
            "user": "user1",
            "start": expected_start,
            "end": expected_end,
            "skipLongRunningNodes": False,
        }
        assert call_kwargs[1]["headers"]["Content-Type"] == "application/json"
        assert "X-Zipline-Version" in call_kwargs[1]["headers"]