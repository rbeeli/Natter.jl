# Connection Auth And TLS

Use one NATS authentication scheme per connection. TLS client certificates are transport-level auth and can be combined with token, user/password, NKEY, or JWT credentials when the deployment requires both.

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

URL userinfo is also supported for token and user/password connections. Do not mix URL credentials with `auth=...` on the same connection.

```julia
token_client = connect("nats://token-value@nats.example.com:4222")
user_client = connect("nats://app:secret@nats.example.com:4222")
```

## NKEY And JWT

Use a seed file when Natter should derive the public NKEY and sign the server nonce:

```julia
nkey_client = connect("nats://nats.example.com:4222";
    auth=NKeyAuth(; seed_path="/etc/nats/user.nk"),
)
```

Use a standard `.creds` file for user JWT auth:

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

For external signers, provide the public user NKEY or JWT plus a callback returning the raw 64-byte Ed25519 signature:

```julia
signed_client = connect("nats://nats.example.com:4222";
    auth=NKeyAuth(;
        nkey=ENV["NATS_NKEY_PUBLIC"],
        signature_cb=nonce -> sign_nonce_with_hsm(nonce),
    ),
)
```

Use `CallbackAuth` when credentials depend on server `INFO`:

```julia
client = connect("nats://nats.example.com:4222";
    auth=CallbackAuth(req -> TokenAuth(token_for(req.url))),
)
```

## TLS And mTLS

`tls://` performs TLS before reading server `INFO`:

```julia
tls_client = connect("tls://nats.example.com:4222";
    tls_ca_path="/etc/nats/ca.pem",
)
```

Client certificates require both certificate and key paths:

```julia
mtls_client = connect("tls://nats.example.com:4222";
    tls_ca_path="/etc/nats/ca.pem",
    tls_cert_path="/etc/nats/client.pem",
    tls_key_path="/etc/nats/client-key.pem",
)
```

For INFO-first TLS upgrades:

```julia
upgrade_client = connect("nats://nats.example.com:4222"; tls_required=true)
info_first_client = connect("tls://nats.example.com:4222"; tls_first=false)
```

Verify a DNS certificate while dialing an IP address:

```julia
named_cert_client = connect("tls://10.0.0.5:4222";
    tls_ca_path="/etc/nats/ca.pem",
    tls_server_name="nats.example.com",
)
```

Disable certificate verification only for controlled development or test environments:

```julia
dev_client = connect("tls://127.0.0.1:4222"; tls_verify=false)
```
