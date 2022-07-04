from datetime import datetime, timedelta

import backoff
import logging
import json
import requests
from base64 import b64encode

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)s - %(levelname)s - %(message)s'
                    )
logger = logging.getLogger(__name__)

HEADERS = {"Content-Type": "application/json"}


class AnbimaClient:
    AUTH_URL = "https://api.anbima.com.br/oauth/access-token"
    AUTH_BODY = {"grant_type": "client_credentials"}

    def __init__(self, client_id, client_secret):
        self.client_id: str = client_id
        self.client_secret: str = client_secret
        self._access_token: str or None = None
        self._last_authorized: datetime or None = None
        self.debentures = self.Debentures(self)
        self.fundos = self.Funds(self)

    @property
    def access_token(self):
        return self._access_token

    @property
    def is_online(self):
        if self._last_authorized and \
                datetime.utcnow() - timedelta(hours=1) < self._last_authorized:
            return True
        return False

    def connect(self):
        def _make_auth_header():
            auth_string = f"{self.client_id}:{self.client_secret}"
            enc_string = b64encode(auth_string.encode()).decode()
            return f"Basic {enc_string}"

        @backoff.on_exception(
            backoff.expo,
            requests.exceptions.RequestException,
            max_time=300
        )
        def _connect(headers):
            r = requests.post(
                url=self.AUTH_URL,
                headers=headers,
                data=json.dumps(self.AUTH_BODY)
            )
            if r.ok:
                logger.info("Connection successful!")
                return r
            if r.status_code == 429:
                logger.warning("We are being rate-limited.")
                raise requests.exceptions.RequestException
            logger.warning("Something went wrong")
            raise Exception

        logger.info(f"Connecting to Anbima with client_id={self.client_id}...")

        auth_payload = _make_auth_header()
        auth_headers = dict(HEADERS, **{"Authorization": auth_payload})
        r_json = _connect(auth_headers).json()
        self._last_authorized = datetime.utcnow()
        self._access_token = r_json.get("access_token")

    def reconnect(self):
        logger.info("Access token expired. Reconnecting...")
        self.connect()

    class Debentures:
        BASE_URL = "https://api.anbima.com.br/feed/precos-indices/v1/debentures/"

        def __init__(self, parent):
            self.parent: AnbimaClient = parent

        @backoff.on_exception(
            backoff.expo,
            requests.exceptions.RequestException,
            max_time=300
        )
        def secondary(self, **kwargs):
            if not self.parent.is_online:
                self.parent.reconnect()
            params = kwargs
            headers = dict(
                HEADERS, **{
                    "client_id": self.parent.client_id,
                    "access_token": self.parent.access_token,
                }
            )

            logger.info(f"Fetching debentures with params = {params}.")

            r = requests.get(
                url=self.BASE_URL+"mercado-secundario",
                headers=headers,
                params=params,
            )
            if r.ok:
                return r.json()
            if r.status_code == 429:
                logger.warning("We are being rate-limited.")
                raise requests.exceptions.RequestException
            raise Exception(f"Something went wrong: {r.status_code=}, {r.text}")

    class Funds:
        BASE_URL = "https://api.anbima.com.br/feed/fundos/v1/"

        def __init__(self, parent):
            self.parent: AnbimaClient = parent

        @backoff.on_exception(
            backoff.expo,
            requests.exceptions.RequestException,
            max_time=300
        )
        def _list_funds(self, _type: str, get_all: bool = False, page: int = 0):
            if not self.parent.is_online:
                self.parent.reconnect()
            url = self.BASE_URL + _type
            headers = dict(
                HEADERS, **{
                    "client_id": self.parent.client_id,
                    "access_token": self.parent.access_token,
                }
            )
            params = {"page": page}
            r = requests.get(url, headers=headers, params=params)
            if r.ok:
                logger.info(f"Fetched page {page} for funds of type: {_type}.")
                data = r.json()
                elements = data.get("total_elements")
                funds = data.get("content")
                if get_all:
                    pagination = int(data.get("size"))
                    for page in range(1, elements//pagination + 1):
                        funds += self._list_funds(
                            _type=_type,
                            page=page,
                        )
                return funds
            if r.status_code == 429:
                logger.warning("We are being rate-limited.")
                raise requests.exceptions.RequestException
            raise Exception(f"Something went wrong: {r.status_code=}, {r.text}")

        def icvm(self, get_all=False):
            return self._list_funds(_type="fundos", get_all=get_all)

        def estruturados(self, get_all=False):
            return self._list_funds(_type="fundos-estruturados", get_all=get_all)

        def offshore(self, get_all=False):
            return self._list_funds(_type="fundos-offshore", get_all=get_all)

        def investidores(self, get_all=False):
            pass
