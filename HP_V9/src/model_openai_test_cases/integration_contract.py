"""Composite mixin for test_model_openai.IntegrationContractTests."""

from .integration_contract_helpers import IntegrationContractHelpersMixin
from .integration_contract_group01 import IntegrationContractGroup01Mixin
from .integration_contract_group02 import IntegrationContractGroup02Mixin
from .integration_contract_group03 import IntegrationContractGroup03Mixin
from .integration_contract_group04 import IntegrationContractGroup04Mixin
from .integration_contract_group05 import IntegrationContractGroup05Mixin
from .integration_contract_group06 import IntegrationContractGroup06Mixin
from .integration_contract_group07 import IntegrationContractGroup07Mixin


class IntegrationContractTestsMixin(
    IntegrationContractHelpersMixin,
    IntegrationContractGroup01Mixin,
    IntegrationContractGroup02Mixin,
    IntegrationContractGroup03Mixin,
    IntegrationContractGroup04Mixin,
    IntegrationContractGroup05Mixin,
    IntegrationContractGroup06Mixin,
    IntegrationContractGroup07Mixin,
):
    pass
