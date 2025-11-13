from azure.identity import (
    DefaultAzureCredential,
    ManagedIdentityCredential,
    ClientSecretCredential,
    EnvironmentCredential,
    AzureCliCredential,
    InteractiveBrowserCredential
)
import os
import logging

class AzureAuthHelper:
    """
    A unified helper for Azure authentication using different methods.
    """

    def __init__(self, method: str = "default", **kwargs):
        """
        Initialize the authentication helper.
        :param method: Authentication method name
                       (default | managed_identity | service_principal |
                        environment | cli | interactive)
        :param kwargs: Optional parameters like tenant_id, client_id, client_secret
        """
        self.method = method.lower()
        self.kwargs = kwargs
        self.credential = None
        self._setup_logger()
        self._init_credential()

    def _setup_logger(self):
        self.logger = logging.getLogger("AzureAuthHelper")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def _init_credential(self):
        """Initialize the appropriate Azure credential."""
        try:
            if self.method == "default":
                self.credential = DefaultAzureCredential(
                    exclude_interactive_browser_credential=False
                )
                self.logger.info("Using DefaultAzureCredential")

            elif self.method == "managed_identity":
                client_id = self.kwargs.get("client_id")
                self.credential = ManagedIdentityCredential(client_id=client_id)
                self.logger.info("Using ManagedIdentityCredential")

            elif self.method == "service_principal":
                self.credential = ClientSecretCredential(
                    tenant_id=self.kwargs["tenant_id"],
                    client_id=self.kwargs["client_id"],
                    client_secret=self.kwargs["client_secret"]
                )
                self.logger.info("Using ClientSecretCredential (Service Principal)")

            elif self.method == "environment":
                self.credential = EnvironmentCredential()
                self.logger.info("Using EnvironmentCredential")

            elif self.method == "cli":
                self.credential = AzureCliCredential()
                self.logger.info("Using AzureCliCredential")

            elif self.method == "interactive":
                self.credential = InteractiveBrowserCredential()
                self.logger.info("Using InteractiveBrowserCredential")

            else:
                raise ValueError(f"Unknown authentication method: {self.method}")

        except Exception as e:
            self.logger.error(f"Failed to initialize credential: {e}")
            raise

    def get_token(self, scope: str = "https://management.azure.com/.default"):
        """
        Retrieve an access token for a specific Azure scope.
        :param scope: The resource scope (default is Azure Management API)
        :return: Access token string
        """
        token = self.credential.get_token(scope)
        self.logger.info(f"Successfully acquired token for scope: {scope}")
        return token.token

    def get_credential(self):
        """
        Get the Azure credential object for SDK clients.
        :return: Azure credential instance
        """
        return self.credential
