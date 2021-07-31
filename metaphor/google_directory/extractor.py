import base64
import json
import logging
from dataclasses import dataclass
from typing import Dict, List

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from metaphor.models.metadata_change_event import (
    EntityType,
    Group,
    GroupID,
    GroupInfo,
    MetadataChangeEvent,
    Person,
    PersonLogicalID,
    PersonProperties,
)
from smart_open import open

from metaphor.common.entity_id import EntityId
from metaphor.common.event_util import EventUtil
from metaphor.common.extractor import BaseExtractor, RunConfig

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# If modifying these scopes, delete the file token.json.
SCOPES = [
    "https://www.googleapis.com/auth/admin.directory.user",
    "https://www.googleapis.com/auth/admin.directory.group",
]


class InvalidTokenError(Exception):
    """Thrown when the OAuth token is no longer valid"""

    def __init__(self):
        super.__init__(self, "Token is no longer valid. Please authenticate again")


@dataclass
class GoogleDirectoryRunConfig(RunConfig):
    token_file: str


def build_directory_service(config: GoogleDirectoryRunConfig):
    with open(config.token_file) as fin:
        credential = Credentials.from_authorized_user_info(json.load(fin), SCOPES)

    # If token expired, try refresh it.
    if not credential.valid:
        if credential.expired and credential.refresh_token:
            credential.refresh(Request())
        else:
            raise InvalidTokenError()

    return build("admin", "directory_v1", credentials=credential)


class GoogleDirectoryExtractor(BaseExtractor):
    """Google Directory metadata extractor"""

    @staticmethod
    def config_class():
        return GoogleDirectoryRunConfig

    def __init__(self):
        self._users: List[Person] = []
        self._groups: List[Group] = []

    async def extract(self, config: RunConfig) -> List[MetadataChangeEvent]:
        assert isinstance(config, GoogleDirectoryExtractor.config_class())

        logger.info("Fetching metadata from Google Directory")

        service = build_directory_service(config)

        # get all users
        results = (
            service.users().list(customer="my_customer", orderBy="email").execute()
        )
        users = results.get("users", [])

        for user in users:
            photo = self._get_photo(service, user["id"])
            self._parse_user(user, photo)

        # get all groups
        results = (
            service.groups().list(customer="my_customer", orderBy="email").execute()
        )
        groups = results.get("groups", [])

        for group in groups:
            # get group members
            response = (
                service.members()
                .list(groupKey=group["email"], includeDerivedMembership=True)
                .execute()
            )
            members = response["members"]

            self._parse_group(group, members)

        logger.debug(json.dumps([p.to_dict() for p in self._users]))
        logger.debug(json.dumps([p.to_dict() for p in self._groups]))

        return [EventUtil.build_person_event(p) for p in self._users] + [
            EventUtil.build_group_event(p) for p in self._groups
        ]

    def _get_photo(self, service, user_id) -> Dict:
        try:
            return service.users().photos().get(userKey=user_id).execute()
        except HttpError as error:
            if error.resp.status != 404:
                raise error

            # user has no photo
            return {}

    def _parse_user(self, user: Dict, photo: Dict) -> None:
        person = Person()
        person.logical_id = PersonLogicalID()
        person.logical_id.email = user["primaryEmail"]

        person.properties = PersonProperties()
        person.properties.first_name = user["name"]["givenName"]
        person.properties.last_name = user["name"]["familyName"]

        if "mimeType" and "photoData" in photo:
            # Convert URL-safe to standard base64 encoding to form a Data URL
            data = base64.standard_b64encode(
                base64.urlsafe_b64decode(photo["photoData"])
            ).decode("utf-8")
            person.properties.avatar_url = f'data:{photo["mimeType"]};base64,{data}'

        self._users.append(person)

    def _parse_group(self, group: Dict, members: List[Dict]) -> None:
        grp = Group()
        grp.logical_id = GroupID()
        grp.logical_id.group_name = group["name"]
        grp.group_info = GroupInfo()
        grp.group_info.email = group["email"]
        grp.group_info.admins = []
        grp.group_info.members = []
        grp.group_info.subgroups = []

        for member in members:
            # TODO: get subgroups instead of derived members
            if member["type"] == "USER":
                user = EntityId(EntityType.PERSON, PersonLogicalID(member["email"]))

                if member["role"] in ["OWNER", "MANAGER"]:
                    grp.group_info.admins.append(str(user))
                else:
                    grp.group_info.members.append(str(user))

        self._groups.append(grp)
