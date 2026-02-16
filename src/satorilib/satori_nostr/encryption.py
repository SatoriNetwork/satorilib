"""Encryption and decryption for Satori Nostr messages using NIP-04.

This module provides simple wrappers around nostr-sdk's encryption
to work with JSON-serialized data.

Note: Currently uses NIP-04 encryption for simplicity. Can be upgraded
to NIP-17/NIP-59 (gift-wrapped events) in the future if needed.
"""
from nostr_sdk import Keys, PublicKey, nip04_encrypt, nip04_decrypt


class EncryptionError(Exception):
    """Raised when encryption or decryption fails."""
    pass


def encrypt_json(
    json_str: str, recipient_pubkey: PublicKey, sender_keys: Keys
) -> str:
    """Encrypt a JSON string for a specific recipient.

    Uses NIP-04 encryption to create an encrypted string that can only
    be decrypted by the recipient using the sender's public key.

    Args:
        json_str: JSON string to encrypt
        recipient_pubkey: Recipient's public key
        sender_keys: Sender's keypair (used for encryption)

    Returns:
        Encrypted string

    Raises:
        EncryptionError: If encryption fails

    Example:
        >>> sender = Keys.generate()
        >>> recipient = Keys.generate()
        >>> json_data = '{"price": 45000}'
        >>> encrypted = encrypt_json(json_data, recipient.public_key(), sender)
    """
    try:
        # Encrypt using NIP-04
        encrypted = nip04_encrypt(
            sender_keys.secret_key(), recipient_pubkey, json_str
        )

        return encrypted

    except Exception as e:
        raise EncryptionError(f"Failed to encrypt JSON: {e}") from e


def decrypt_json(
    encrypted: str, sender_pubkey: PublicKey, recipient_keys: Keys
) -> str:
    """Decrypt an encrypted JSON string.

    Uses NIP-04 decryption to recover the original JSON string.

    Args:
        encrypted: Encrypted string from encrypt_json()
        sender_pubkey: Sender's public key (used for decryption)
        recipient_keys: Recipient's keypair

    Returns:
        Decrypted JSON string

    Raises:
        EncryptionError: If decryption fails

    Example:
        >>> sender = Keys.generate()
        >>> recipient = Keys.generate()
        >>> encrypted = "..."  # from encrypt_json
        >>> json_str = decrypt_json(encrypted, sender.public_key(), recipient)
    """
    try:
        # Decrypt using NIP-04
        json_str = nip04_decrypt(
            recipient_keys.secret_key(), sender_pubkey, encrypted
        )

        return json_str

    except Exception as e:
        raise EncryptionError(f"Failed to decrypt JSON: {e}") from e


# Helper functions for encrypting/decrypting specific model types
def encrypt_observation(observation_json: str, recipient_pubkey: PublicKey, sender_keys: Keys) -> str:
    """Encrypt a DatastreamObservation JSON string."""
    return encrypt_json(observation_json, recipient_pubkey, sender_keys)


def decrypt_observation(encrypted: str, sender_pubkey: PublicKey, recipient_keys: Keys) -> str:
    """Decrypt a DatastreamObservation JSON string."""
    return decrypt_json(encrypted, sender_pubkey, recipient_keys)


def encrypt_payment(payment_json: str, recipient_pubkey: PublicKey, sender_keys: Keys) -> str:
    """Encrypt a PaymentNotification JSON string."""
    return encrypt_json(payment_json, recipient_pubkey, sender_keys)


def decrypt_payment(encrypted: str, sender_pubkey: PublicKey, recipient_keys: Keys) -> str:
    """Decrypt a PaymentNotification JSON string."""
    return decrypt_json(encrypted, sender_pubkey, recipient_keys)
