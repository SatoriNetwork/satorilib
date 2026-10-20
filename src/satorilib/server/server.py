'''
Here's plan for the server - python server, you checkin with it,
it returns a key you use to make a websocket connection with the pubsub server.

# TODO:
- [ ] implement DTOs for all the server calls
- [ ] implement Swagger on the server / python packages...
{
    "DTO": "Proposal",
    "error": null,
    "data": {
        "id": 1,
        "author": "22a85fb71485c6d7c62a3784c5549bd3849d0afa3ee44ce3f9ea5541e4c56402d8",
        "title": "Proposal Title",
        "description": "Proposal Description",
        ...
    }
}
JSON -> EXTRACT DATA -> Python Object -> DTO -> JSON
{{ proposal.author }}
'''
from typing import Union, Optional
from functools import partial
import base64
import time
import json
import requests
from datetime import datetime, timedelta
from satorilib import logging
from satorilib.utils.time import timeToTimestamp
from satorilib.wallet import Wallet
from satorilib.concepts.structs import Stream, StreamId
from satorilib.server.api import ProposalSchema, VoteSchema
from requests.exceptions import RequestException
import json
import traceback
import datetime as dt


class SatoriServerClient(object):
    def __init__(
        self,
        wallet: Wallet,
        url: str = None,
        sendingUrl: str = None,
        *args, **kwargs
    ):
        self.wallet = wallet
        # Use central config for URL
        from satorilib.config import get_central_url
        default_url = get_central_url()
        self.url = url or default_url
        self.sendingUrl = sendingUrl or default_url
        self.topicTime: dict[str, float] = {}
        self.lastCheckin: int = 0
        # JWT token storage
        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None

    def setTopicTime(self, topic: str):
        self.topicTime[topic] = time.time()

    def _getChallenge(self):
        """Get challenge token from central-lite or fallback to timestamp."""
        try:
            response = requests.get(self.url + '/api/v1/auth/challenge', timeout=10)
            if response.status_code == 200:
                challenge = response.json().get('challenge')
                if challenge:
                    return challenge
                else:
                    logging.warning(
                        'Challenge endpoint returned empty challenge, using timestamp fallback',
                        color='yellow')
            else:
                logging.warning(
                    f'Challenge endpoint returned status {response.status_code}, using timestamp fallback',
                    color='yellow')
        except Exception as e:
            logging.warning(
                f'Failed to fetch challenge from server: {str(e)}. Using timestamp fallback.',
                color='yellow')
        return str(time.time())

    def _login_with_jwt(self):
        """Perform JWT login with an EIP-191 wallet signature."""
        try:
            # Generate challenge (timestamp)
            challenge = str(time.time())

            # Call login endpoint
            response = requests.post(
                self.url + '/api/v1/auth/login',
                headers={
                    'wallet-pubkey': self.wallet.pubkey,
                    'message': challenge,
                    'signature': self.wallet.ethSign(challenge)
                },
                timeout=15
            )
            response.raise_for_status()

            data = response.json()
            self._access_token = data['access_token']
            self._refresh_token = data['refresh_token']
            self._token_expiry = datetime.now() + timedelta(seconds=data['expires_in'])

            logging.info('JWT login successful', print=True)
        except Exception as e:
            logging.error(f'JWT login failed: {e}', color='red')
            raise

    def _refresh_jwt_token(self):
        """Refresh access token using refresh token."""
        if not self._refresh_token:
            raise Exception("No refresh token available")

        try:
            response = requests.post(
                self.url + '/api/v1/auth/refresh',
                headers={'Authorization': f'Bearer {self._refresh_token}'},
                timeout=15
            )
            response.raise_for_status()

            data = response.json()
            self._access_token = data['access_token']
            self._token_expiry = datetime.now() + timedelta(seconds=data['expires_in'])

            logging.info('JWT token refreshed', print=True)
        except Exception as e:
            logging.error(f'JWT token refresh failed: {e}', color='red')
            raise

    def _ensure_authenticated(self):
        """Ensure we have a valid access token."""
        now = datetime.now()

        # No token? Login
        if not self._access_token or not self._token_expiry:
            self._login_with_jwt()
            return

        # Token expired? Refresh or re-login
        if now >= self._token_expiry:
            if self._refresh_token:
                try:
                    self._refresh_jwt_token()
                except Exception:
                    self._login_with_jwt()
            else:
                self._login_with_jwt()
            return

        # Token expiring soon (within 5 minutes)? Proactive refresh
        if now >= (self._token_expiry - timedelta(minutes=5)):
            if self._refresh_token:
                try:
                    self._refresh_jwt_token()
                except Exception:
                    pass  # Current token still valid

    def _makeAuthenticatedCall(
        self,
        function: callable,
        endpoint: str,
        url: str = None,
        payload: Union[str, dict, None] = None,
        challenge: str = None,
        useWallet: Wallet = None,
        extraHeaders: Union[dict, None] = None,
        raiseForStatus: bool = True,
    ) -> requests.Response:
        if isinstance(payload, dict):
            payload = json.dumps(payload)

        if payload is not None:
            logging.info(
                f'outgoing: {endpoint}',
                payload[0:40], f'{"..." if len(payload) > 40 else ""}',
                print=True)

        # Try JWT authentication first
        try:
            self._ensure_authenticated()

            headers = {
                'Authorization': f'Bearer {self._access_token}',
                **(extraHeaders or {}),
            }

            # Add Content-Type header if payload is present
            if payload is not None:
                headers['Content-Type'] = 'application/json'

            r = function(
                (url or self.url) + endpoint,
                headers=headers,
                data=payload,
                timeout=15)

            # If 401, token might be expired - retry once with fresh token
            if r.status_code == 401:
                logging.warning('JWT auth failed, retrying with fresh token')
                self._login_with_jwt()
                headers['Authorization'] = f'Bearer {self._access_token}'
                r = function(
                    (url or self.url) + endpoint,
                    headers=headers,
                    data=payload,
                    timeout=15)

            if raiseForStatus and not r.ok:
                logging.error(
                    f'server error on {endpoint}: {r.status_code}', color='red')
                r.raise_for_status()

            logging.info(
                f'incoming: {endpoint}',
                r.text[0:40], f'{"..." if len(r.text) > 40 else ""}',
                print=True)
            return r

        except Exception as e:
            # Fall back to legacy challenge-response.
            # This handles both JWT auth failures and transient server errors
            # (the legacy call is effectively a retry with different auth headers).
            is_server_error = (
                isinstance(e, requests.exceptions.HTTPError)
                and e.response is not None
                and e.response.status_code >= 500)
            if is_server_error:
                logging.warning(
                    f'server error on {endpoint} ({e.response.status_code}), '
                    f'retrying with legacy auth')
            else:
                logging.warning(f'JWT auth failed, falling back to legacy: {e}')

            headers = {
                **(useWallet or self.wallet).authPayload(
                    asDict=True,
                    challenge=challenge or self._getChallenge()),
                **(extraHeaders or {}),
            }

            # Add Content-Type header if payload is present
            if payload is not None:
                headers['Content-Type'] = 'application/json'

            r = function(
                (url or self.url) + endpoint,
                headers=headers,
                data=payload,
                timeout=15)
            if raiseForStatus and not r.ok:
                logging.error(
                    f'server error on {endpoint}: {r.status_code}', color='red')
                r.raise_for_status()
            logging.info(
                f'incoming: {endpoint}',
                r.text[0:40], f'{"..." if len(r.text) > 40 else ""}',
                print=True)
            return r

    def _makeUnauthenticatedCall(
        self,
        function: callable,
        endpoint: str,
        url: str = None,
        headers: Union[dict, None] = None,
        payload: Union[str, bytes, None] = None,
    ):
        logging.info(
            'outgoing Satori server message to ',
            endpoint,
            print=True)
        data = None
        json = None
        if isinstance(payload, bytes):
            headers = headers or {'Content-Type': 'application/octet-stream'}
            data = payload
        elif isinstance(payload, str):
            headers = headers or {'Content-Type': 'application/json'}
            json = payload
        else:
            headers = headers or {}
        r = function(
            (url or self.url) + endpoint,
            headers=headers,
            json=json,
            data=data)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logging.error("unauth'ed server err:", r.text, e, color='red')
            r.raise_for_status()
        logging.info(
            'incoming Satori server message:',
            r.text[0:40], f'{"..." if len(r.text) > 40 else ""}',
            print=True)
        return r

    def registerWallet(self):
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/register/wallet',
            payload=self.wallet.registerPayload())

    def registerStream(self, stream: dict, payload: str = None):
        ''' publish stream {'source': 'test', 'name': 'stream1', 'target': 'target'}'''
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/register/stream',
            payload=payload or json.dumps(stream))

    def registerSubscription(self, subscription: dict, payload: str = None):
        ''' subscribe to stream '''
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/register/subscription',
            payload=payload or json.dumps(subscription))

    def registerPin(self, pin: dict, payload: str = None):
        '''
        report a pin to the server.
        example: {
            'author': {'pubkey': '22a85fb71485c6d7c62a3784c5549bd3849d0afa3ee44ce3f9ea5541e4c56402d8'},
            'stream': {'source': 'satori', 'pubkey': '22a85fb71485c6d7c62a3784c5549bd3849d0afa3ee44ce3f9ea5541e4c56402d8', 'stream': 'stream1', 'target': 'target', 'cadence': None, 'offset': None, 'datatype': None, 'url': None, 'description': 'raw data'},,
            'ipns': 'ipns',
            'ipfs': 'ipfs',
            'disk': 1,
            'count': 27},
        '''
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/register/pin',
            payload=payload or json.dumps(pin))

    def requestPrimary(self):
        ''' subscribe to primary data stream and and publish prediction '''
        return self._makeAuthenticatedCall(
            function=requests.get,
            endpoint='/request/primary')

    def getStreams(self, stream: dict, payload: str = None):
        ''' subscribe to primary data stream and and publish prediction '''
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/get/streams',
            payload=payload or json.dumps(stream))

    def myStreams(self):
        ''' subscribe to primary data stream and and publish prediction '''
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/my/streams',
            payload='{}')

    def removeStream(self, stream: dict = None, payload: str = None):
        ''' removes a stream from the server '''
        if payload is None and stream is None:
            raise ValueError('stream or payload must be provided')
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/remove/stream',
            payload=payload or json.dumps(stream or {}))
    
    def restoreStream(self, stream: dict = None, payload: str = None):
        ''' removes a stream from the server '''
        if payload is None and stream is None:
            raise ValueError('stream or payload must be provided')
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/restore/stream',
            payload=payload or json.dumps(stream or {}))

    def checkin(self, vaultInfo: dict = None, nostrPubkey: str = None, version: str = None) -> dict:
        """Check in with central server. For central-lite, uses auth challenge system."""
        challenge = self._getChallenge()

        # Register peer with central-lite (no health check needed - registration will fail if server is down)
        try:
            # For central-lite: Register peer with vault info and nostr pubkey
            # Call /api/v1/peer/register with wallet-pubkey, vault-pubkey, nostr-pubkey headers
            if vaultInfo and vaultInfo.get('vaultpubkey'):
                try:
                    headers = {
                        'wallet-pubkey': self.wallet.pubkey,
                        'vault-pubkey': vaultInfo.get('vaultpubkey'),
                    }
                    if nostrPubkey:
                        headers['nostr-pubkey'] = nostrPubkey
                    if version:
                        headers['neuron-version'] = version
                    register_response = requests.post(
                        self.url + '/api/v1/peer/register',
                        headers=headers,
                        timeout=10
                    )
                    if register_response.status_code == 200:
                        pass
                    else:
                        logging.warning(f'peer registration failed: {register_response.text}')
                except Exception as e:
                    logging.warning(f'peer registration error: {e}')

            # Return minimal checkin data for central-lite
            self.lastCheckin = time.time()
            return {
                'wallet': {
                    'accepting': False,
                    'rewardaddress': None,
                },
                'key': challenge,
                'oracleKey': challenge,
                'idKey': challenge,
                'subscriptionKeys': [],
                'publicationKeys': [],
                'subscriptions': '[]',
                'publications': '[]',
                'pins': '[]',
                'stakeRequired': 0,
                'rewardaddress': None,
            }
        except Exception as e:
            logging.error(f'central-lite connection failed: {e}', color='red')
            raise Exception(f'Unable to connect to central-lite server: {e}')

    def checkinCheck(self) -> bool:
        """Check if there are updates since last checkin. Returns True if health check fails."""
        try:
            health_response = requests.get(self.url + '/health')
            if health_response.status_code == 200:
                return False  # healthy - no restart needed
            else:
                logging.warning(f'central-lite health check returned status {health_response.status_code}')
                return True  # unhealthy - trigger restart
        except Exception as e:
            logging.warning(f'central-lite health check failed in checkinCheck: {e}')
            return True  # failed - trigger restart

    def getRelays(self) -> list[dict]:
        """Get all verified relays from central server.

        Returns list of dicts: {relay_url, nostr_pubkey}
        """
        r = self._makeAuthenticatedCall(
            function=requests.get,
            endpoint='/api/v1/peer/relays')
        return r.json()

    def getRelayConfig(self) -> dict:
        """Get server-driven relay/neuron configuration (e.g. max_streams).

        Returns an empty dict if the endpoint is unavailable so callers
        can fall back to local defaults. Does not log the response body
        (the values it returns are caps the server controls, not data
        the user should see surfaced and tempted to override locally).
        """
        try:
            r = requests.get(self.url + '/api/v1/peer/relay-config', timeout=15)
            if not r.ok:
                logging.warning(
                    f'relay config fetch failed: status {r.status_code}')
                return {}
            data = r.json()
            if not isinstance(data, dict):
                logging.warning('relay config fetch failed: bad payload')
                return {}
            logging.info('relay config fetch ok', print=True)
            return data
        except Exception as e:
            logging.warning(f'relay config fetch failed: {e}')
            return {}


    def requestSimplePartial(self, network: str):
        ''' sends a satori partial transaction to the server '''
        return self._makeUnauthenticatedCall(
            function=requests.get,
            url=self.sendingUrl,
            endpoint=f'/simple_partial/request/{network}').json()

    def broadcastSimplePartial(
        self,
        tx: bytes,
        feeSatsReserved: float,
        reportedFeeSats: float,
        walletId: float,
        network: str
    ):
        ''' sends a satori partial transaction to the server '''
        return self._makeUnauthenticatedCall(
            function=requests.post,
            url=self.sendingUrl,
            endpoint=f'/simple_partial/broadcast/{network}/{feeSatsReserved}/{reportedFeeSats}/{walletId}',
            payload=tx)

    def broadcastBridgeSimplePartial(
        self,
        tx: bytes,
        feeSatsReserved: float,
        reportedFeeSats: float,
        walletId: float,
        network: str
    ):
        ''' sends a satori partial transaction to the server '''
        return self._makeUnauthenticatedCall(
            function=requests.post,
            url=self.sendingUrl,
            endpoint=f'/simple/bridge/partial/broadcast/{network}/{feeSatsReserved}/{reportedFeeSats}/{walletId}',
            payload=tx)

    def removeWalletAlias(self):
        ''' removes the wallet alias from the server '''
        return self._makeAuthenticatedCall(
            function=requests.get,
            endpoint='/remove_wallet_alias')

    def updateWalletAlias(self, alias: str):
        ''' removes the wallet alias from the server '''
        return self._makeAuthenticatedCall(
            function=requests.get,
            endpoint='/update_wallet_alias/' + alias)

    def getWalletAlias(self):
        ''' removes the wallet alias from the server '''
        return self._makeAuthenticatedCall(
            function=requests.get,
            endpoint='/get_wallet_alias').text

    def getManifestVote(self, wallet: Wallet = None):
        return self._makeUnauthenticatedCall(
            function=requests.get,
            endpoint=(
                f'/votes_for/manifest/{wallet.publicKey}'
                if isinstance(wallet, Wallet) else '/votes_for/manifest')).json()

    def getSanctionVote(self, wallet: Wallet = None, vault: Wallet = None):
        # logging.debug('vault', vault, color='yellow')
        walletPubkey = wallet.publicKey if isinstance(
            wallet, Wallet) else 'None'
        vaultPubkey = vault.publicKey if isinstance(vault, Wallet) else 'None'
        # logging.debug(
        #    f'/votes_for/sanction/{walletPubkey}/{vaultPubkey}', color='yellow')
        return self._makeUnauthenticatedCall(
            function=requests.get,
            endpoint=f'/votes_for/sanction/{walletPubkey}/{vaultPubkey}').json()

    def marketStreamsSetPrice(self, streamUuid: str = None, pricePerObs: float = None) -> bool:
        """
        Set the price per observation for a stream.
        
        Args:
            streamUuid: A StreamUuid we wish to set the price for
            pricePerObs: The price per observation we wish to set
            
        Returns:
            bool: True if the price per observation request was successful, False otherwise
        """
        try:
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/market/streams/set/price',
                payload=json.dumps({
                    'streamUuid': streamUuid,
                    'pricePerObs': pricePerObs}))
            return response.status_code == 200
        except Exception as e:
            logging.error(f"Error setting price per observation: {str(e)}")
            return False

    def getCentrifugoToken(self) -> dict:
        """
        Get the centrifugo token for the user.
        
        Returns: {
            "token": token,
            "ws_url": CENTRIFUGO_WS_URL,
            "expires_at": expires_at.isoformat() + "Z",
            "user_id": user_id}
        """
        try:
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint='/centrifugo/token')
            if response.status_code == 200:
                return response.json()
            else:
                return {}
        except Exception as e:
            logging.error(f"Error setting price per observation: {str(e)}")
            return False

    def marketBuyStream(self, streamUuid: str = None) -> bool:
        """
        Buy a stream by sending a request to the server.
        
        Args:
            streamUuid: A StreamUuid we wish to buy
            
        Returns:
            bool: True if the buy request was successful, False otherwise
        """
        try:
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/market/streams/buy',
                payload=json.dumps({'streamUuid': streamUuid}))
            return response.status_code == 200
        except Exception as e:
            logging.error(f"Error predicting stream: {str(e)}")
            return False

    def incrementVote(self, streamId: str):
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/vote_on/sanction/incremental',
            payload=json.dumps({'streamId': streamId})).text

    def removeVote(self, streamId: str):
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/clear_vote_on/sanction/incremental',
            payload=json.dumps({'streamId': streamId})).text

    def predictStream(self, streamId: int) -> bool:
        """
        Start predicting a stream by sending a request to the server.
        
        Args:
            streamId: A StreamId object containing the stream details to predict
            
        Returns:
            bool: True if the prediction request was successful, False otherwise
        """
        try:
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/request/stream/specific',
                payload=json.dumps({'streamId': streamId}))
            return response.status_code == 200
        except Exception as e:
            logging.error(f"Error predicting stream: {str(e)}")
            return False

    def flagStream(self, streamId: int) -> bool:
        """
        Flag a stream as inappropriate or bad by sending a request to the server.
        Args:
            streamId: The stream ID to flag
        Returns:
            bool: True if the flag request was successful, False otherwise
        """
        try:
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/flag/stream',
                payload=json.dumps({'streamId': streamId}))
            return response.status_code == 200
        except Exception as e:
            logging.error(f"Error flagging stream: {str(e)}")
            return False

    def getObservations(self, streamId: str):
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/observations/list',
            payload=json.dumps({'streamId': streamId})).text

    def getPowerBalance(self):
        response = self._makeAuthenticatedCall(
            function=requests.get,
            endpoint='/api/v1/balance/get').json()
        return response['stake']
    
    def submitMaifestVote(self, wallet: Wallet, votes: dict[str, int]):
        # todo authenticate the vault instead
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/vote_on/manifest',
            useWallet=wallet,
            payload=json.dumps(votes or {})).text

    def submitSanctionVote(self, wallet: Wallet, votes: dict[str, int]):
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/vote_on/sanction',
            useWallet=wallet,
            payload=json.dumps(votes or {})).text

    def removeSanctionVote(self, wallet: Wallet):
        return self._makeAuthenticatedCall(
            function=requests.Get,
            endpoint='/clear_votes_on/sanction',
            useWallet=wallet).text

    def pinDepinStream(self, stream: dict = None) -> tuple[bool, str]:
        ''' removes a stream from the server '''
        if stream is None:
            raise ValueError('stream must be provided')
        response = self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/register/subscription/pindepin',
            payload=json.dumps(stream))
        if response.status_code < 400:
            return response.json().get('success'), response.json().get('result')
        return False, ''

    def mineToAddressStatus(self) -> Union[str, None]:
        ''' get reward address from central server using v1 API (no auth required) '''
        try:
            # No authentication needed - query by wallet_pubkey
            response = requests.get(
                self.url + f'/api/v1/peer/reward-address?wallet_pubkey={self.wallet.pubkey}',
                timeout=10)

            # 404 is expected for new peers that haven't registered yet
            if response.status_code == 404:
                return ''

            if response.status_code > 399:
                logging.warning(
                    f'Failed to get reward address: {response.status_code} {response.text}',
                    color='yellow')
                return None

            # Parse JSON response
            data = response.json()
            reward_address = data.get('reward_address', '')
            if not reward_address or reward_address in ['null', 'None', 'NULL']:
                return ''
            return reward_address
        except Exception as e:
            logging.warning(
                'unable to get reward address; try again Later.', e, color='yellow')
            return None

    def setRewardAddress(
        self,
        address: str,
    ) -> tuple[bool, str]:
        '''
        Set reward address for authenticated peer.

        Simplified to only require the address - auth is handled via
        wallet headers from _makeAuthenticatedCall. A refusal (400 "Invalid
        Base address") comes back as (False, body), not as an auth failure.
        '''
        try:
            # Call new v1 API with simplified payload
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/api/v1/peer/reward-address',
                payload=json.dumps({'reward_address': address}),
                raiseForStatus=False)
            return response.status_code < 400, response.text
        except Exception as e:
            logging.warning(
                'unable to set reward address; try again Later.', e, color='yellow')
            return False, ''

    def lendRemove(self) -> tuple[bool, dict]:
        ''' removes a stream from the server '''
        try:
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint='/stake/lend/remove')
            return response.status_code < 400, response.text
        except Exception as e:
            logging.warning(
                'unable to stakeProxyRemove due to connection timeout; try again Later.', e, color='yellow')
            return False, {}

    def lendAddress(self) -> Union[str, None]:
        ''' get lending address '''
        try:
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint='/stake/lend/address')
            if response.status_code > 399:
                return 'Unknown'
            if response.text in ['null', 'None', 'NULL']:
                return ''
            return response.text
        except Exception as e:
            logging.warning(
                'unable to get reward address; try again Later.', e, color='yellow')
            return ''

    def fetchWalletStatsDaily(self) -> str:
        ''' gets wallet stats '''
        try:
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint='/wallet/stats/daily')
            return response.json()
        except Exception as e:
            logging.warning(
                'unable to disable status of Mine-To-Vault feature due to connection timeout; try again Later.', e, color='yellow')
            return ''

    def stakeCheck(self) -> bool:
        ''' gets wallet stats '''
        try:
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint='/stake/check')
            if response.text == 'TRUE':
                return True
        except Exception as e:
            logging.warning(
                'unable to disable status of Mine-To-Vault feature due to connection timeout; try again Later.', e, color='yellow')
            return False
        return False

    def poolAddressRemove(self, lend_id: str):
        return self._makeAuthenticatedCall(
            function=requests.post,
            endpoint='/stake/lend/address/remove',
            payload=json.dumps({'lend_id': lend_id})).text

    def delegateGet(self) -> tuple[bool, str]:
        ''' my delegate '''
        try:
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint='/stake/proxy/delegate')
            return response.status_code < 400, response.text
        except Exception as e:
            logging.warning(
                'unable to delegateGet due to connection timeout; try again Later.', e, color='yellow')
            return False, {}

    def delegateRemove(self) -> tuple[bool, str]:
        ''' my delegate '''
        try:
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint='/stake/proxy/delegate/remove')
            return response.status_code < 400, response.text
        except Exception as e:
            logging.warning(
                'unable to delegateRemove due to connection timeout; try again Later.', e, color='yellow')
            return False, {}

    def publish(
        self,
        topic: str,
        data: str,
        observationTime: str,
        observationHash: str,
        isPrediction: bool = True,
        useAuthorizedCall: bool = True,
    ) -> Union[bool, None]:
        ''' publish predictions '''
        #logging.info(f'publishing', color='blue')
        # if not isPrediction and self.topicTime.get(topic, 0) > time.time() - (Stream.minimumCadence*.95):
        #    return
        # if isPrediction and self.topicTime.get(topic, 0) > time.time() - 60*60:
        #    return
        lastPublishTime = self.topicTime.get(topic, 0)
        timeSinceLastPublish = time.time() - lastPublishTime
        minInterval = Stream.minimumCadence * 0.95

        if timeSinceLastPublish < minInterval:
            timeUntilNext = int(minInterval - timeSinceLastPublish)
            minutesUntilNext = timeUntilNext // 60
            secondsUntilNext = timeUntilNext % 60
            logging.debug(
                f'Rate limited: skipping {"prediction" if isPrediction else "observation"} publish '
                f'(next publish in {minutesUntilNext}m {secondsUntilNext}s)',
                color='cyan')
            return
        self.setTopicTime(topic)
        try:
            if isPrediction:
                # Call our new v1 API for predictions
                response = self._makeAuthenticatedCall(
                    function=requests.post,
                    endpoint='/api/v1/prediction/post',
                    payload=json.dumps({
                        'value': str(data),
                        'observed_at': str(observationTime),
                        'hash': str(observationHash),
                    }))
            else:
                # Observations not yet supported by our API
                # TODO: Implement /api/v1/observation/post endpoint
                logging.warning('Observation publishing not yet supported by API', color='yellow')
                return None

            if response.status_code == 200:
                return True
            if response.status_code > 399:
                logging.warning(
                    f'Prediction rejected with status {response.status_code}: {response.text}',
                    color='yellow')
                return None
            if response.text.lower() in ['fail', 'null', 'none', 'error']:
                logging.warning(f'Prediction failed: {response.text}', color='yellow')
                return False
        except Exception as e:
            logging.warning(
                f'Unable to publish prediction: {str(e)}. Will retry later.',
                color='yellow')
            return None
        return True

    def publishPredictionsBatch(self, predictions: list[dict]) -> Union[dict, None]:
        """
        Publish multiple predictions in a single batch request.

        Args:
            predictions: List of prediction dicts, each containing:
                - stream_uuid: Stream UUID this prediction is for
                - stream_name: Optional stream name for logging
                - value: Predicted value
                - observed_at: Timestamp
                - hash: Hash for data integrity

        Returns:
            dict with keys: total_submitted, successful, failed, prediction_ids, errors
            None if request fails

        Example:
            >>> predictions = [
            ...     {"stream_uuid": "abc-123", "stream_name": "btc", "value": "45000", "observed_at": "...", "hash": "..."},
            ...     {"stream_uuid": "def-456", "stream_name": "eth", "value": "3000", "observed_at": "...", "hash": "..."}
            ... ]
            >>> result = server.publishPredictionsBatch(predictions)
            >>> print(f"Submitted {result['successful']}/{result['total_submitted']} predictions")
        """
        try:
            if not predictions:
                logging.warning("No predictions to submit in batch", color='yellow')
                return None

            # Call batch prediction endpoint
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/api/v1/predictions/batch',
                payload=json.dumps({'predictions': predictions}),
                raiseForStatus=False
            )

            if response.status_code == 200:
                result = response.json()
                logging.info(
                    f"Batch prediction: {result['successful']}/{result['total_submitted']} successful",
                    color='green')
                return result
            elif response.status_code > 399:
                logging.warning(
                    f'Batch prediction rejected with status {response.status_code}: {response.text}',
                    color='yellow')
                return None
            else:
                logging.warning(f'Batch prediction unexpected response: {response.text}', color='yellow')
                return None

        except Exception as e:
            logging.error(f'Failed to submit batch predictions: {e}', color='red')
            return None

    def getObservationsBatch(self, storage=None, limit: int = 100) -> Union[list, None]:
        """
        Get the latest batch of observations from the Central Server.

        This retrieves all observations from the most recent daily run, including:
        - Multi-crypto observations (btc, eth, doge, etc.)
        - SafeTrade observations (safetrade_btc, etc.)
        - Bitcoin observation (bitcoin)

        Args:
            storage: Optional storage manager for storing stream metadata
            limit: Maximum number of observations to return (default: 100)

        Returns:
            List of observation dicts with stream_uuid added, or None if request fails
        """
        try:
            # Call new batch endpoint
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint=f'/api/v1/observations/batch?limit={limit}',
                raiseForStatus=False
            )
            if response.status_code == 200:
                observations = response.json()
                if observations is None:
                    return None

                # Process each observation to extract and store stream metadata
                processed_observations = []
                for data in observations:
                    # Extract and store stream metadata if present
                    if data.get('stream'):
                        stream_info = data['stream']
                        stream_uuid = stream_info.get('uuid')

                        if stream_uuid and storage and hasattr(storage, 'db'):
                            # Store stream metadata in client's streams table
                            try:
                                storage.db.upsertStream(
                                    uuid=stream_uuid,
                                    server_stream_id=stream_info.get('id'),
                                    name=stream_info.get('name'),
                                    author=stream_info.get('author'),
                                    secondary=stream_info.get('secondary'),
                                    target=stream_info.get('target'),
                                    meta=stream_info.get('meta'),
                                    description=stream_info.get('description')
                                )
                                # Add stream_uuid to observation for easy access
                                data['stream_uuid'] = stream_uuid
                            except Exception as e:
                                logging.warning(
                                    f"Failed to store stream metadata for {stream_info.get('name')}: {e}",
                                    color='yellow')

                    processed_observations.append(data)

                if processed_observations:
                    logging.info(
                        f"Retrieved {len(processed_observations)} observations from latest batch",
                        color='green')

                return processed_observations
            else:
                logging.warning(
                    f"Failed to get observations batch. Status code: {response.status_code}",
                    color='yellow')
                return None
        except Exception as e:
            logging.error(
                f"Error occurred while fetching observations batch: {str(e)}",
                color='red')
            return None

    def getObservation(self, stream: str = 'bitcoin', storage=None) -> Union[dict, None]:
        """
        Get the latest observation from the Central Server.

        Args:
            stream: The stream/topic to get observations for (default: 'bitcoin')
                    NOTE: This is a temporary testing endpoint - will be updated later
            storage: Optional storage manager for storing stream metadata

        Returns:
            dict with keys: observation_id, value, observed_at, ts, bitcoin_price, sources,
                           stream_uuid (if stream metadata is present)
            None if request fails or no observation available
        """
        try:
            # NOTE: Using GET /api/v1/observation/get endpoint
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint='/api/v1/observation/get',
                raiseForStatus=False
            )
            if response.status_code == 200:
                data = response.json()
                if data is None:
                    return None

                # Extract and store stream metadata if present
                if data.get('stream'):
                    stream_info = data['stream']
                    stream_uuid = stream_info.get('uuid')

                    if stream_uuid and storage and hasattr(storage, 'db'):
                        # Store stream metadata in client's streams table
                        try:
                            storage.db.upsertStream(
                                uuid=stream_uuid,
                                server_stream_id=stream_info.get('id'),
                                name=stream_info.get('name'),
                                author=stream_info.get('author'),
                                secondary=stream_info.get('secondary'),
                                target=stream_info.get('target'),
                                meta=stream_info.get('meta'),
                                description=stream_info.get('description')
                            )
                            # Add stream_uuid to response for easy access
                            data['stream_uuid'] = stream_uuid
                            logging.info(
                                f"Stored stream metadata for '{stream_info.get('name')}' (UUID: {stream_uuid})",
                                color='green')
                        except Exception as e:
                            logging.warning(
                                f"Failed to store stream metadata: {e}",
                                color='yellow')

                return data
            else:
                logging.warning(
                    f"Failed to get observation for stream '{stream}'. Status code: {response.status_code}",
                    color='yellow')
                return None
        except Exception as e:
            logging.error(
                f"Error occurred while fetching observation for stream '{stream}': {str(e)}",
                color='red')
            return None

    # def getProposalById(self, proposal_id: str) -> dict:
    #    try:
    #        response = self._makeUnauthenticatedCall(
    #            function=requests.get,
    #            endpoint=f'/proposals/get/{proposal_id}'  # Update endpoint path
    #        )
    #        if response.status_code == 200:
    #            return response.json()
    #        else:
    #            logging.error(f"Failed to get proposal. Status code: {response.status_code}")
    #            return None
    #    except Exception as e:
    #        logging.error(f"Error occurred while fetching proposal: {str(e)}")
    #        return None

    def getProposals(self):
        """
        Function to get all proposals by calling the API endpoint.
        """
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint='/proposals/get/all'
            )
            if response.status_code == 200:
                proposals = response.json()
                return proposals
            else:
                logging.error(
                    f"Failed to get proposals. Status code: {response.status_code}", color='red')
                return []
        except requests.RequestException as e:
            logging.error(
                f"Error occurred while fetching proposals: {str(e)}", color='red')
            return []

    def getApprovedProposals(self):
        """
        Function to get all approved proposals by calling the API endpoint.
        """
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint='/proposals/get/approved'
            )
            if response.status_code == 200:
                proposals = response.json()
                return proposals
            else:
                logging.error(
                    f"Failed to get approved proposals. Status code: {response.status_code}", color='red')
                return []
        except requests.RequestException as e:
            logging.error(
                f"Error occurred while fetching approved proposals: {str(e)}", color='red')
            return []

    def submitProposal(self, proposal_data: dict) -> tuple[bool, dict]:
        '''submits proposal'''
        try:
            # Ensure options is a JSON string
            if 'options' in proposal_data and isinstance(proposal_data['options'], list):
                proposal_data['options'] = json.dumps(proposal_data['options'])

            # Convert the entire proposal_data to a JSON string
            proposal_json_string = json.dumps(proposal_data)

            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/proposal/submit',
                payload=proposal_json_string
            )
            if response.status_code < 400:
                return True, response.text
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                logging.error(f"Error in submitProposal: {error_message}")
                return False, {"error": error_message}

        except RequestException as re:
            error_message = f"Request error in submitProposal: {str(re)}"
            logging.error(error_message)
            logging.error(traceback.format_exc())
            return False, {"error": error_message}
        except Exception as e:
            error_message = f"Unexpected error in submitProposal: {str(e)}"
            logging.error(error_message)
            logging.error(traceback.format_exc())
            return False, {"error": error_message}

    def getProposalById(self, proposal_id: str) -> dict:
        """
        Function to get a specific proposal by ID by calling the API endpoint.
        """
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint=f'/proposal/{proposal_id}'
            )
            if response.status_code == 200:
                return response.json()['proposal']
            else:
                logging.error(
                    f"Failed to get proposal. Status code: {response.status_code}",
                    extra={'color': 'red'}
                )
                return None
        except requests.RequestException as e:
            logging.error(
                f"Error occurred while fetching proposal: {str(e)}",
                extra={'color': 'red'}
            )
            return None

    def getProposalVotes(self, proposal_id: str, format_type: str = None) -> dict:
        '''Gets proposal votes with option for raw or processed format'''
        try:
            endpoint = f'/proposal/votes/get/{proposal_id}'
            if format_type:
                endpoint += f'?format={format_type}'

            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint=endpoint
            )

            if response.status_code == 200:
                return response.json()
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return {'status': 'error', 'message': error_message}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    def getExpiredProposals(self) -> dict:
        """
        Fetches expired proposals
        """
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint='/proposals/expired'
            )
            if response.status_code == 200:
                return {'status': 'success', 'proposals': response.json()}
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return {'status': 'error', 'message': error_message}
        except Exception as e:
            error_message = f"Error in getExpiredProposals: {str(e)}"
            return {'status': 'error', 'message': error_message}

    def getActiveProposals(self) -> dict:
        """
        Fetches active proposals
        """
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint='/proposals/active'
            )
            if response.status_code == 200:
                return {'status': 'success', 'proposals': response.json()}
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return {'status': 'error', 'message': error_message}
        except Exception as e:
            error_message = f"Error in getActiveProposals: {str(e)}"
            return {'status': 'error', 'message': error_message}

    def submitProposalVote(self, proposal_id: int, vote: str) -> tuple[bool, dict]:
        """
        Submits a vote for a proposal
        """
        try:
            vote_data = {
                "proposal_id": int(proposal_id),  # Send proposal_id as integer
                "vote": str(vote),
            }
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/proposal/vote/submit',
                payload=vote_data  # Pass the vote_data dictionary directly
            )
            if response.status_code == 200:
                return True, response.text
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}

        except Exception as e:
            error_message = f"Error in submitProposalVote: {str(e)}"
            return False, {"error": error_message}

    def poolAccepting(self, status: bool) -> tuple[bool, dict]:
        """
        Function to set the pool status to accepting or not accepting
        """
        try:
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint='/stake/lend/enable' if status else '/stake/lend/disable')
            if response.status_code == 200:
                return True, response.text
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            error_message = f"Error in poolAccepting: {str(e)}"
            return False, {"error": error_message}

    ## untested ##

    def setMiningMode(self, status: bool) -> tuple[bool, dict]:
        """
        Function to set the worker mining mode
        """
        try:
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint='/worker/mining/mode/enable' if status else '/worker/mining/mode/disable')
            if response.status_code == 200:
                return True, response.text
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            error_message = f"Error in setMiningMode: {str(e)}"
            return False, {"error": error_message}

    def loopbackCheck(self, ipAddress:Union[str, None], port: Union[int, None]) -> bool:
        """
        asks the central server (could ask fellow Neurons) if our own dataserver
        is publically reachable.
        """
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.post,
                endpoint='/api/v0/loopback/check',
                payload=json.dumps({
                    **({'ip': str(ipAddress)} if ipAddress is not None else {}),
                    **({'port': port} if port is not None else {})}))
            if response.status_code == 200:
                try:
                    return response.json().get('port_open', False)
                except Exception as e:
                    return False
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False
        except Exception as e:
            error_message = f"Error in setMiningMode: {str(e)}"
            return False

    def getSubscribers(self) -> tuple[bool, list]:
        """
        asks the central server (could ask fellow Neurons) if our own dataserver
        is publically reachable.
        """
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint='/api/v0/get/subscribers')
            if response.status_code == 200:
                return True, response.json()
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            error_message = f"Error in setMiningMode: {str(e)}"
            return False, {"error": error_message}

    def getStreamsSubscribers(self, streams:list[str]) -> tuple[bool, list]:
        """
        asks the central server (could ask fellow Neurons) if our own dataserver
        is publically reachable.
        """
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.post,
                endpoint='/api/v0/get/stream/subscribers',
                payload=json.dumps({'streams': streams}))
            if 200 <= response.status_code < 400:
                return True, response.json()
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            error_message = f"Error in setMiningMode: {str(e)}"
            return False, {"error": error_message}

    def getStreamsPublishers(self, streams:list[str]) -> tuple[bool, list]:
        """
        asks the central server (could ask fellow Neurons) if our own dataserver
        is publically reachable.
        """
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.post,
                endpoint='/api/v0/get/stream/publisher',
                payload=json.dumps({'streams': streams}))
            if 200 <= response.status_code < 400:
                return True, response.json()
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            error_message = f"Error in setMiningMode: {str(e)}"
            return False, {"error": error_message}

    def getContentCreated(self) -> tuple[bool, dict]:
        try:
            response = self._makeUnauthenticatedCall(
                function=requests.get,
                endpoint='/api/v0/content/created/get')
            if response.status_code == 200:
                return True, response.json()
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            error_message = f"Error in getContentCreated: {str(e)}"
            return False, {"error": error_message}

    def approveInviters(self, approved: list[int]) -> tuple[bool, list]:
        try:
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/api/v0/inviters/approve',
                payload=json.dumps({"approved": approved}))
            if response.status_code == 200:
                return True, response.text
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            return False, {"error": f"Error in setMiningMode: {str(e)}"}

    def disapproveInviters(self, disapproved: list[int]) -> tuple[bool, dict]:
        try:
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/api/v0/inviters/disapprove',
                payload=json.dumps({"disapproved": disapproved}))
            if response.status_code == 200:
                return True, response.text
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            error_message = f"Error in setMiningMode: {str(e)}"
            return False, {"error": error_message}

    def deleteContent(self, deleted: list[int]) -> tuple[bool, str]:
        try:
            response = self._makeAuthenticatedCall(
                function=requests.post,
                endpoint='/api/v0/content/delete',
                payload=json.dumps({"deleted": deleted}))
            if response.status_code == 200:
                return True, response.text
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            error_message = f"Error in setMiningMode: {str(e)}"
            return False, {"error": error_message}

    def getBalances(self) -> tuple[bool, dict]:
        try:
            response = self._makeAuthenticatedCall(
                function=requests.get,
                endpoint='/api/v0/balances/get')
            if response.status_code == 200:
                return True, response.json()
            else:
                error_message = f"Server returned status code {response.status_code}: {response.text}"
                return False, {"error": error_message}
        except Exception as e:
            error_message = f"Error in setMiningMode: {str(e)}"
            return False, {"error": error_message}