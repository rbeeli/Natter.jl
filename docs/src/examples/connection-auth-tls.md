# Connection Auth And TLS

These examples show the supported connection security modes. Use one NATS authentication scheme per connection: token, user/password, NKEY, or user JWT credentials. TLS client certificates are transport-level authentication and can be used with a NATS auth scheme when the deployment requires both.

## Token And User Password

```julia
using Natter

token_client = connect("nats://nats.example.com:4222";
    token=ENV["NATS_TOKEN"],
)

user_client = connect("nats://nats.example.com:4222";
    user=ENV["NATS_USER"],
    password=ENV["NATS_PASSWORD"],
)
```

Token and user/password credentials can also be supplied in URL userinfo. Do not mix URL credentials with option credentials on the same connection.

```julia
token_client = connect("nats://token-value@nats.example.com:4222")
user_client = connect("nats://app:secret@nats.example.com:4222")
```

## NKEY And JWT Credentials

Use a seed file when Natter should derive the public NKEY and sign the server nonce:

```julia
nkey_client = connect("nats://nats.example.com:4222";
    nkey_seed_path="/etc/nats/user.nk",
)
```

Use a standard decorated `.creds` file for user JWT auth:

```julia
creds_client = connect("nats://nats.example.com:4222";
    credentials_path="/etc/nats/user.creds",
)
```

Separate JWT and seed files are also supported:

```julia
jwt_client = connect("nats://nats.example.com:4222";
    jwt_path="/etc/nats/user.jwt",
    nkey_seed_path="/etc/nats/user.nk",
)
```

For HSM or external signer integrations, provide the public NKEY or JWT plus a callback that returns the raw 64-byte Ed25519 signature:

```julia
signed_client = connect("nats://nats.example.com:4222";
    nkey=ENV["NATS_NKEY_PUBLIC"],
    signature_cb=nonce -> sign_nonce_with_hsm(nonce),
)
```

## TLS Encryption

TLS is the supported transport encryption mode. `tls://` performs TLS before reading the server `INFO` line.

```julia
tls_client = connect("tls://nats.example.com:4222";
    tls_ca_path="/etc/nats/ca.pem",
)
```

Client certificate authentication requires both certificate and key paths:

```julia
mtls_client = connect("tls://nats.example.com:4222";
    tls_ca_path="/etc/nats/ca.pem",
    tls_cert_path="/etc/nats/client.pem",
    tls_key_path="/etc/nats/client-key.pem",
)
```

For deployments that advertise TLS in `INFO` before upgrading, use `nats://` with `tls_required=true`, or force INFO-first behavior on a `tls://` URL with `tls_first=false`.

```julia
upgrade_client = connect("nats://nats.example.com:4222"; tls_required=true)
info_first_client = connect("tls://nats.example.com:4222"; tls_first=false)
```

Certificate verification is enabled by default. Disable it only in controlled development or test environments:

```julia
dev_client = connect("tls://127.0.0.1:4222"; tls_verify=false)
```
