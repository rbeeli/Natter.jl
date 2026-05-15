# Connection Auth And TLS

These examples show the supported connection security modes. Use one NATS authentication scheme per connection: token, user/password, NKEY, or user JWT credentials. TLS client certificates are transport-level authentication and can be used with a NATS auth scheme when the deployment requires both.

## Token And User Password

```julia
using Natter

token_client = connect("nats://nats.example.com:4222";
    auth=TokenAuth(ENV["NATS_TOKEN"]),
)

user_client = connect("nats://nats.example.com:4222";
    auth=UserPassAuth(ENV["NATS_USER"], ENV["NATS_PASSWORD"]),
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
    auth=NKeyAuth(; seed_path="/etc/nats/user.nk"),
)
```

Use a standard decorated `.creds` file for user JWT auth:

```julia
creds_client = connect("nats://nats.example.com:4222";
    auth=CredentialsAuth(; path="/etc/nats/user.creds"),
)
```

Separate JWT and seed files are also supported:

```julia
jwt_client = connect("nats://nats.example.com:4222";
    auth=JwtAuth(; jwt_path="/etc/nats/user.jwt", seed_path="/etc/nats/user.nk"),
)
```

For HSM or external signer integrations, provide the public NKEY or JWT plus a callback that returns the raw 64-byte Ed25519 signature:

```julia
signed_client = connect("nats://nats.example.com:4222";
    auth=NKeyAuth(;
        nkey=ENV["NATS_NKEY_PUBLIC"],
        signature_cb=nonce -> sign_nonce_with_hsm(nonce),
    ),
)
```

Use `CallbackAuth` when credentials need to be chosen after server `INFO` is available:

```julia
client = connect("nats://nats.example.com:4222";
    auth=CallbackAuth(req -> TokenAuth(token_for(req.url))),
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

Certificate verification uses the URL host by default. IP-literal URLs must be covered by an IP subject alternative name in the server certificate. Use `tls_server_name` when connecting to an address but verifying a DNS certificate name:

```julia
named_cert_client = connect("tls://10.0.0.5:4222";
    tls_ca_path="/etc/nats/ca.pem",
    tls_server_name="nats.example.com",
)
```

Certificate verification is enabled by default. Disable it only in controlled development or test environments:

```julia
dev_client = connect("tls://127.0.0.1:4222"; tls_verify=false)
```
