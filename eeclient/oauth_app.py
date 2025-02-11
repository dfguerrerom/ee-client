"""This file requires to install .[dev] to run the code"""

from flask import Flask, request
import requests
import json

app = Flask(__name__)

CLIENT_SECRET_FILE = "/home/dguerrero/Downloads/client_secret.json"


try:
    with open(CLIENT_SECRET_FILE, "r") as f:
        client_secrets = json.load(f)
        CLIENT_ID = client_secrets["installed"]["client_id"]
        CLIENT_SECRET = client_secrets["installed"]["client_secret"]
except FileNotFoundError:
    print(
        f"Error: client_secret.json not found at {CLIENT_SECRET_FILE}. Please set the CLIENT_SECRET_FILE environment variable or place the file in the same directory as this script."
    )
    exit(1)  # Exit if the file isn't found

REDIRECT_URI = "http://localhost:8080/callback"
SCOPES = ["https://www.googleapis.com/auth/earthengine"]
AUTHORIZATION_URL = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"


@app.route("/")
def index():
    auth_url = build_authorization_url()
    return f'<a href="{auth_url}">Authorize with Google Earth Engine</a>'


def build_authorization_url():
    authorization_url = f"{AUTHORIZATION_URL}?client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}&scope={' '.join(SCOPES)}&response_type=code&access_type=offline&prompt=consent"
    return authorization_url


@app.route("/callback")
def callback():
    authorization_code = request.args.get("code")
    if authorization_code:
        tokens = exchange_code_for_tokens(authorization_code)
        if tokens:
            refresh_token = tokens.get("refresh_token")
            if refresh_token:
                print(
                    f"Refresh Token: {refresh_token}"
                )  # For testing ONLY!  Store securely in production.
                return "Refresh token received. Check your console."
            else:
                return "No refresh token received."
        else:
            return "Error exchanging tokens."
    else:
        return "Authorization code not received."


def exchange_code_for_tokens(authorization_code):
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code": authorization_code,
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code",
    }
    response = requests.post(TOKEN_URL, data=data)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        print(f"Token exchange error: {response.text}")
        return None


if __name__ == "__main__":
    app.run(debug=True, port=8080)
