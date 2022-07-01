from dataclasses import field
from typing import Collection, Dict, List, Optional, Set

try:
    from slack_sdk import WebClient
except ImportError:
    print("Please install metaphor[slack] extra\n")
    raise

from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    Person,
    PersonLogicalID,
    PersonSlackProfile,
)
from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.extractor import BaseExtractor
from metaphor.common.logger import get_logger

logger = get_logger(__name__)


@dataclass
class SlackRunConfig(BaseConfig):
    oauth_token: str

    # How many users to fetch per page
    page_size: int = 100

    # Include deleted users
    include_deleted: bool = False

    # Include restricted (guest) users
    include_restricted: bool = False

    # Exclude the default Slack bot, which is weirdly not marked as "is_bot".
    excluded_ids: Set[str] = field(default_factory=lambda: {"USLACKBOT"})


def list_all_users(config: SlackRunConfig) -> List[Dict]:
    users = []
    next_cursor = None
    client = WebClient(token=config.oauth_token)
    while True:
        response = client.users_list(limit=config.page_size, cursor=next_cursor)
        users.extend(response["members"])
        next_cursor = response["response_metadata"].get("next_cursor", "")
        if next_cursor == "":
            return users


class SlackExtractor(BaseExtractor):
    """Slack directory extractor"""

    def platform(self) -> Optional[Platform]:
        return None

    def description(self) -> str:
        return "Slack directory crawler"

    @staticmethod
    def config_class():
        return SlackRunConfig

    async def extract(self, config: SlackRunConfig) -> Collection[ENTITY_TYPES]:
        assert isinstance(config, SlackExtractor.config_class())

        logger.info("Fetching directory data from Slack")

        persons = []
        for user in list_all_users(config):
            # Filter out users based on the config
            if user["is_bot"]:
                continue

            if user["deleted"] and not config.include_deleted:
                continue

            if user["id"] in config.excluded_ids:
                continue

            if (
                user["is_restricted"] or user["is_ultra_restricted"]
            ) and not config.include_restricted:
                continue

            slack_profile = PersonSlackProfile(
                slack_id=user["id"],
                team_id=user["team_id"],
                username=user["name"],
                deleted=user["deleted"],
            )

            email = user["profile"].get("email", None)
            if email is None:
                logger.warn(f"Skipping user {user['id']} without email address")
                continue

            persons.append(
                Person(
                    logical_id=PersonLogicalID(email=email),
                    slack_profile=slack_profile,
                )
            )

        return persons
