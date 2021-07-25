import argparse
import os

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

# If modifying these scopes, delete the file token.json.
SCOPES = [
    "https://www.googleapis.com/auth/admin.directory.user",
    "https://www.googleapis.com/auth/admin.directory.group",
]


def authenticate(credential_file, token_file):
    workspace = os.path.dirname(os.path.realpath(credential_file))
    token_file = workspace + "/token.json"

    if not os.path.exists(credential_file):
        raise Exception("Not credential.json")

    credential = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(token_file):
        credential = Credentials.from_authorized_user_file(token_file, SCOPES)

    # If there are no (valid) credentials available, let the user log in.
    if not credential or not credential.valid:
        if credential and credential.expired and credential.refresh_token:
            credential.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credential_file, SCOPES)
            credential = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(token_file, "w") as token:
            token.write(credential.to_json())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Google Directory API authenticator")
    parser.add_argument("credential", help="Path to the OAuth 2 Client credential file")
    parser.add_argument(
        "token",
        help="Path to output the token file. A refresh will be attempted if pointed to an existing file.",
    )
    args = parser.parse_args()

    authenticate(args.credential, args.token)
