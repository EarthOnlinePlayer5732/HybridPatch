"""Composite mixin for test_model_openai.DeepSeekOpenCodeCampaignTests."""

from .deepseek_campaign_helpers import DeepSeekOpenCodeCampaignHelpersMixin
from .deepseek_campaign_group01 import DeepSeekOpenCodeCampaignGroup01Mixin
from .deepseek_campaign_group02 import DeepSeekOpenCodeCampaignGroup02Mixin
from .deepseek_campaign_group03 import DeepSeekOpenCodeCampaignGroup03Mixin
from .deepseek_campaign_group04 import DeepSeekOpenCodeCampaignGroup04Mixin
from .deepseek_campaign_group05 import DeepSeekOpenCodeCampaignGroup05Mixin
from .deepseek_campaign_group06 import DeepSeekOpenCodeCampaignGroup06Mixin


class DeepSeekOpenCodeCampaignTestsMixin(
    DeepSeekOpenCodeCampaignHelpersMixin,
    DeepSeekOpenCodeCampaignGroup01Mixin,
    DeepSeekOpenCodeCampaignGroup02Mixin,
    DeepSeekOpenCodeCampaignGroup03Mixin,
    DeepSeekOpenCodeCampaignGroup04Mixin,
    DeepSeekOpenCodeCampaignGroup05Mixin,
    DeepSeekOpenCodeCampaignGroup06Mixin,
):
    pass
