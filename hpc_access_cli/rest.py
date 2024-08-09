"""Code for interfacing with the hpc-access REST API."""

from typing import List

import httpx

from hpc_access_cli.config import HpcaccessSettings
from hpc_access_cli.models import (
    HpcGroup,
    HpcProject,
    HpcUser,
    ResourceData,
    ResourceDataUser,
)


class HpcaccessClient:
    """Client for accessing the hpc-access REST API."""

    def __init__(self, settings: HpcaccessSettings):
        #: The settings to use.
        self.settings = settings
        self.base_url = settings.server_url
        self.client = httpx.Client(base_url=str(settings.server_url))

    def load_users(self) -> List[HpcUser]:
        """Load users from the hpc-access server."""
        url = "adminsec/api/hpcuser/"
        headers = {"Authorization": f"Token {self.settings.api_token.get_secret_value()}"}
        result = []
        while True:
            response = self.client.get(url, headers=headers)
            response.raise_for_status()
            response_data = response.json()
            for entry in response_data.get("results", []):
                result.append(HpcUser.model_validate(entry))
            if response_data.get("next"):
                url = str(response_data.get("next"))
                url = url[len(str(self.base_url)) :]
            else:
                break
        return result

    def load_groups(self) -> List[HpcGroup]:
        """Load groups from the hpc-access server."""
        url = "adminsec/api/hpcgroup/"
        headers = {"Authorization": f"Token {self.settings.api_token.get_secret_value()}"}
        result = []
        while True:
            response = self.client.get(url, headers=headers)
            response.raise_for_status()
            response_data = response.json()
            for entry in response_data.get("results", []):
                result.append(HpcGroup.model_validate(entry))
            if response_data.get("next"):
                url = str(response_data.get("next"))
                url = url[len(str(self.base_url)) :]
            else:
                break
        return result

    def load_projects(self) -> List[HpcProject]:
        """Load projects from the hpc-access server."""
        url = "adminsec/api/hpcproject/"
        headers = {"Authorization": f"Token {self.settings.api_token.get_secret_value()}"}
        result = []
        while True:
            response = self.client.get(url, headers=headers)
            response.raise_for_status()
            response_data = response.json()
            for entry in response_data.get("results", []):
                result.append(HpcProject.model_validate(entry))
            if response_data.get("next"):
                url = str(response_data.get("next"))
                url = url[len(str(self.base_url)) :]
            else:
                break
        return result

    def update_user_resources_used(self, user: HpcUser):
        """Update resource usage for a user."""
        url = f"adminsec/api/hpcuser/{user.uuid}/"
        headers = {"Authorization": f"Token {self.settings.api_token.get_secret_value()}"}
        resources_used = user.resources_used or ResourceDataUser().model_dump()
        data = {
            "resources_used": resources_used,
        }
        response = self.client.patch(url, headers=headers, json=data)
        response.raise_for_status()

    def update_group_resources_used(self, group: HpcGroup):
        """Update resource usage for a group."""
        url = f"adminsec/api/hpcgroup/{group.uuid}/"
        headers = {"Authorization": f"Token {self.settings.api_token.get_secret_value()}"}
        resources_used = group.resources_used or ResourceData().model_dump()
        data = {
            "resources_used": resources_used,
        }
        response = self.client.patch(url, headers=headers, json=data)
        response.raise_for_status()

    def update_project_resources_used(self, project: HpcProject):
        """Update resource usage for a project."""
        url = f"adminsec/api/hpcproject/{project.uuid}/"
        headers = {"Authorization": f"Token {self.settings.api_token.get_secret_value()}"}
        resources_used = project.resources_used or ResourceData().model_dump()
        data = {
            "resources_used": resources_used,
        }
        response = self.client.patch(url, headers=headers, json=data)
        response.raise_for_status()

    def close(self):
        self.client.close()
