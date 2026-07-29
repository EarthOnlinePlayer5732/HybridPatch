"""Compatibility entry point for the HP_V9 infrastructure regression suite."""

import unittest

from model_openai_test_cases.deepseek_opencode_campaign import (
    DeepSeekOpenCodeCampaignTestsMixin,
)
from model_openai_test_cases.integration_contract import (
    IntegrationContractTestsMixin,
)
from model_openai_test_cases.minimax_official_transport import (
    MinimaxOfficialTransportTestsMixin,
)
from model_openai_test_cases.opencode_transport import OpenCodeTransportTestsMixin
from model_openai_test_cases.opencode_zen_deepseek import (
    OpenCodeZenDeepSeekTestsMixin,
)


class OpenCodeTransportTests(OpenCodeTransportTestsMixin, unittest.TestCase):
    pass


class IntegrationContractTests(IntegrationContractTestsMixin, unittest.TestCase):
    pass


class OpenCodeZenDeepSeekTests(OpenCodeZenDeepSeekTestsMixin, unittest.TestCase):
    pass


class DeepSeekOpenCodeCampaignTests(
    DeepSeekOpenCodeCampaignTestsMixin,
    unittest.TestCase,
):
    pass


class MinimaxOfficialTransportTests(
    MinimaxOfficialTransportTestsMixin,
    unittest.TestCase,
):
    pass


if __name__ == "__main__":
    unittest.main(verbosity=2)
