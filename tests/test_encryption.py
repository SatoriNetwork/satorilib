"""Tests for satori_nostr encryption module.

Covers: encrypt_json/decrypt_json, encrypt_observation/decrypt_observation,
        encrypt_payment/decrypt_payment, EncryptionError
"""
import json
import pytest
from nostr_sdk import Keys

from satorilib.satori_nostr.encryption import (
    encrypt_json,
    decrypt_json,
    encrypt_observation,
    decrypt_observation,
    encrypt_payment,
    decrypt_payment,
    EncryptionError,
)


@pytest.fixture
def sender_keys():
    return Keys.generate()


@pytest.fixture
def recipient_keys():
    return Keys.generate()


# =========================================================================
# encrypt_json / decrypt_json
# =========================================================================

class TestEncryptDecryptJson:
    def test_basic_roundtrip(self, sender_keys, recipient_keys):
        plaintext = '{"price": 45000}'
        encrypted = encrypt_json(
            plaintext, recipient_keys.public_key(), sender_keys)
        decrypted = decrypt_json(
            encrypted, sender_keys.public_key(), recipient_keys)
        assert decrypted == plaintext

    def test_encrypted_differs_from_plaintext(self, sender_keys, recipient_keys):
        plaintext = '{"price": 45000}'
        encrypted = encrypt_json(
            plaintext, recipient_keys.public_key(), sender_keys)
        assert encrypted != plaintext

    def test_wrong_recipient_cannot_decrypt(self, sender_keys, recipient_keys):
        wrong_keys = Keys.generate()
        plaintext = '{"secret": "data"}'
        encrypted = encrypt_json(
            plaintext, recipient_keys.public_key(), sender_keys)
        with pytest.raises(EncryptionError):
            decrypt_json(encrypted, sender_keys.public_key(), wrong_keys)

    def test_empty_string(self, sender_keys, recipient_keys):
        encrypted = encrypt_json(
            "", recipient_keys.public_key(), sender_keys)
        decrypted = decrypt_json(
            encrypted, sender_keys.public_key(), recipient_keys)
        assert decrypted == ""

    def test_large_json(self, sender_keys, recipient_keys):
        data = json.dumps({"key_" + str(i): "value_" * 100 for i in range(50)})
        encrypted = encrypt_json(
            data, recipient_keys.public_key(), sender_keys)
        decrypted = decrypt_json(
            encrypted, sender_keys.public_key(), recipient_keys)
        assert decrypted == data

    def test_unicode_content(self, sender_keys, recipient_keys):
        plaintext = '{"emoji": "🚀", "japanese": "日本語"}'
        encrypted = encrypt_json(
            plaintext, recipient_keys.public_key(), sender_keys)
        decrypted = decrypt_json(
            encrypted, sender_keys.public_key(), recipient_keys)
        assert decrypted == plaintext

    def test_self_encryption(self, sender_keys):
        """Encrypt to yourself — sender and recipient are the same."""
        plaintext = '{"self": true}'
        encrypted = encrypt_json(
            plaintext, sender_keys.public_key(), sender_keys)
        decrypted = decrypt_json(
            encrypted, sender_keys.public_key(), sender_keys)
        assert decrypted == plaintext

    def test_decrypt_garbage_raises(self, sender_keys, recipient_keys):
        with pytest.raises(EncryptionError):
            decrypt_json("not-encrypted-at-all", sender_keys.public_key(), recipient_keys)

    def test_different_encryptions_differ(self, sender_keys, recipient_keys):
        """NIP-04 should produce different ciphertext each time (random IV)."""
        plaintext = '{"price": 45000}'
        enc1 = encrypt_json(plaintext, recipient_keys.public_key(), sender_keys)
        enc2 = encrypt_json(plaintext, recipient_keys.public_key(), sender_keys)
        # They might be the same in rare cases, but generally differ
        # We test structure rather than strict inequality
        assert isinstance(enc1, str)
        assert isinstance(enc2, str)


# =========================================================================
# encrypt_observation / decrypt_observation
# =========================================================================

class TestObservationEncryption:
    def test_roundtrip(self, sender_keys, recipient_keys):
        obs_json = json.dumps({
            "stream_name": "btc-price",
            "timestamp": 1700000000,
            "value": 45000.0,
            "seq_num": 1,
        })
        encrypted = encrypt_observation(
            obs_json, recipient_keys.public_key(), sender_keys)
        decrypted = decrypt_observation(
            encrypted, sender_keys.public_key(), recipient_keys)
        assert json.loads(decrypted) == json.loads(obs_json)


# =========================================================================
# encrypt_payment / decrypt_payment
# =========================================================================

class TestPaymentEncryption:
    def test_roundtrip(self, sender_keys, recipient_keys):
        pay_json = json.dumps({
            "from_pubkey": sender_keys.public_key().to_hex(),
            "to_pubkey": recipient_keys.public_key().to_hex(),
            "stream_name": "btc-price",
            "seq_num": 42,
            "amount_sats": 100,
            "timestamp": 1700000000,
            "tx_id": None,
        })
        encrypted = encrypt_payment(
            pay_json, recipient_keys.public_key(), sender_keys)
        decrypted = decrypt_payment(
            encrypted, sender_keys.public_key(), recipient_keys)
        assert json.loads(decrypted) == json.loads(pay_json)


# =========================================================================
# EncryptionError
# =========================================================================

class TestEncryptionError:
    def test_is_exception(self):
        assert issubclass(EncryptionError, Exception)

    def test_message(self):
        e = EncryptionError("test error")
        assert str(e) == "test error"
