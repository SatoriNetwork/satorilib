'''The neuron's client for the Satori Network's server (Central): sign in, register, reward address, predictions, observations.'''
from typing import Union, Optional
import time
import json
import requests
from datetime import datetime, timedelta
from satorilib import logging
from satorilib.wallet import Wallet
from satorilib.concepts.structs import Stream


class SatoriServerClient(object):
    def __init__(
        self,
        alias: Wallet,
        url: str = None,
        *args, **kwargs
    ):
        # the alias: the neuron's automated signing key, which it keeps
        # unencrypted; it is who this client is to the Satori Network
        self.alias = alias
        # Use central config for URL
        from satorilib.config import get_central_url
        default_url = get_central_url()
        self.url = url or default_url
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
        """Perform JWT login with an EIP-191 signature by the alias."""
        try:
            # Generate challenge (timestamp)
            challenge = str(time.time())

            # Call login endpoint
            response = requests.post(
                self.url + '/api/v1/auth/login',
                headers={
                    'alias-pubkey': self.alias.pubkey,
                    'message': challenge,
                    'signature': self.alias.ethSign(challenge)
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
        signer: Wallet = None,
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
                **(signer or self.alias).authPayload(
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

    def register(
        self,
        walletLink: Union[dict, None] = None,
        nostrPubkey: str = None,
        version: str = None,
    ) -> dict:
        """Register the alias with the Satori Network (POST /api/v1/peer/register).

        Headers: alias-pubkey, what it is for (alias-scoring,
        alias-reported-by: satorilib.wallet.link.ALIAS_REPORT), nostr-pubkey,
        neuron-version, the wallet's link (wallet-link, wallet-signature;
        see satorilib.wallet.link) when there is one, and the alias's proof:
        message (a fresh challenge, used up by the register) and signature
        (the alias's EIP-191 signature of it). Central needs the proof to move
        the alias to another wallet or replace its Nostr key. Without a link
        the alias registers alone.

        Returns the answer: wallet_address is the linked wallet's Base
        address (or None), and wallet_link_error says why a sent link was
        not accepted. Raises on a refusal or no answer.
        """
        from satorilib.wallet.link import ALIAS_REPORT, linkHeaders
        headers = {'alias-pubkey': self.alias.pubkey, **ALIAS_REPORT}
        if nostrPubkey:
            headers['nostr-pubkey'] = nostrPubkey
        if version:
            headers['neuron-version'] = version
        headers.update(linkHeaders(walletLink))
        challenge = self._getChallenge()
        headers['message'] = challenge
        headers['signature'] = self.alias.ethSign(challenge)
        response = requests.post(
            self.url + '/api/v1/peer/register',
            headers=headers,
            timeout=10)
        if response.status_code != 200:
            raise Exception(
                f'peer registration failed ({response.status_code}): {response.text}')
        return response.json()

    def checkin(
        self,
        walletLink: Union[dict, None] = None,
        nostrPubkey: str = None,
        version: str = None,
    ) -> dict:
        """Check in with central: register the alias (with the wallet's link when given).

        The answer carries walletAddress (the linked wallet's Base address, or
        None) and walletLinkError (why a sent link was refused, or None).
        """
        challenge = self._getChallenge()
        answer = {}
        try:
            answer = self.register(
                walletLink=walletLink,
                nostrPubkey=nostrPubkey,
                version=version) or {}
            if answer.get('wallet_link_error'):
                logging.warning(
                    f"the Satori Network did not accept the wallet's link: "
                    f"{answer.get('wallet_link_error')}")
        except Exception as e:
            logging.warning(f'peer registration error: {e}')
        self.lastCheckin = time.time()
        return {
            'wallet': {
                'accepting': False,
                'rewardaddress': None,
            },
            'walletAddress': answer.get('wallet_address'),
            'walletLinkError': answer.get('wallet_link_error'),
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

    def getRelays(self, forPubkey: Union[str, None] = None) -> list[dict]:
        """Get relays from central server.

        Without forPubkey: every relay central has seen traffic on, as
        {relay_url, nostr_pubkey, address}.

        With forPubkey (this neuron's Nostr public key, hex): this neuron's own
        list, in the order to take them, each also with `healthy`. The neuron
        holds the first `keep` of them (getRelayConfig). An empty list means
        central has no plan for it.
        """
        endpoint = '/api/v1/peer/relays'
        if forPubkey:
            endpoint += f'?for={forPubkey}'
        r = self._makeAuthenticatedCall(
            function=requests.get,
            endpoint=endpoint)
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

    def mineToAddressStatus(self) -> Union[str, None]:
        ''' get reward address from central server using v1 API (no auth required) '''
        try:
            # No authentication needed - query by wallet_pubkey
            response = requests.get(
                self.url + f'/api/v1/peer/reward-address?wallet_pubkey={self.alias.pubkey}',
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
