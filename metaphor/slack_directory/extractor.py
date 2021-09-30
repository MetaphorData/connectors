import logging
from dataclasses import dataclass, field
from typing import Dict, List, Set

try:
    from slack_sdk import WebClient
except ImportError:
    print("Please install metaphor[slack] extra\n")
    raise

from metaphor.models.metadata_change_event import (
    MetadataChangeEvent,
    Person,
    PersonLogicalID,
    PersonSlackProfile,
)
from serde import deserialize

from metaphor.common.event_util import EventUtil
from metaphor.common.extractor import BaseExtractor, RunConfig

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@deserialize
@dataclass
class SlackRunConfig(RunConfig):
    oauth_token: str

    # How many users to fetch per page
    page_size: int = 100

    # Include deleted users
    include_deleted: bool = False

    # Include restricted (guest) users
    include_restricted: bool = False

    # Exclude the default Slack bot, which is weirdly not marked as "is_bot".
    excluded_ids: Set[str] = field(default_factory=lambda: set(["USLACKBOT"]))


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

    @staticmethod
    def config_class():
        return SlackRunConfig

    async def extract(self, config: RunConfig) -> List[MetadataChangeEvent]:
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
                logging.warn(f"Skipping user {user['id']} without email address")
                continue

            persons.append(
                Person(
                    logical_id=PersonLogicalID(email=email),
                    slack_profile=slack_profile,
                )
            )

        return [EventUtil.build_person_event(p) for p in persons]
