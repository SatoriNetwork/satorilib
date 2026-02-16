# Wallet merge notes

## authenticate.py: `pubkey` vs `wallet-pubkey`
- Central's version uses `'pubkey'` as the dict key in `authPayload()` — this is in the request BODY for legacy challenge-response auth
- Neuron changed it to `'wallet-pubkey'` to match the HTTP header name
- Central's API reads pubkey from HTTP HEADER (`wallet-pubkey`), not the body
- Kept as `'pubkey'` (central's version) since legacy auth flow expects it
- The HTTP header `wallet-pubkey` is set separately in `server.py`
