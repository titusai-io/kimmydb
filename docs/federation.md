# Federation — using KimmyDB with your OAuth2 / OIDC identity provider

KimmyDB can delegate authentication to any standards-conformant OAuth 2.0 /
OpenID Connect provider — Entra ID, Okta, Keycloak, Auth0, or a self-hosted
OAuth2/OIDC provider — while authorization stays local: your provider says
**who** the caller is, and this database's role mappings say what they may do.
Local user accounts keep working alongside it, unchanged — and
`auth.local.login` decides from where they may still log in, so the
break-glass root can stay a host-only door
([Security](security.md#local-login-is-a-mode)).

The mechanics live in [Security](security.md#two-ways-in-one-decision). This
page is the task-oriented version: how to wire a provider up, per-provider
notes, and what to check when it does not work.

---

## The five-minute version

Four things, all of them settable through environment variables (compose,
swarm, kubernetes) or the config file:

```sh
# The node trusts exactly one external issuer...
export KIMMY_OIDC_ISSUER=https://auth.example.com
# ...accepts only tokens minted for this audience...
export KIMMY_OIDC_AUDIENCE=https://kimmydb.example.com
# ...reads roles from this claim ("groups" for Entra ID)...
export KIMMY_OIDC_ROLES_CLAIM=roles
# ...and maps claim values to what they are worth here:
export KIMMY_OIDC_ROLE_MAPPINGS='[{"claim_value":"user","grants":[{"db":"*","collection":"*","actions":["read","write","watch","search"]}]}]'
```

```sh
kimmyd check-config     # refuses exactly what the server would refuse;
                        # reaches out to the provider's discovery endpoint too
```

On the provider side, two registrations:

1. **This node as a resource** the provider will mint tokens *for* — its RFC
   8707 resource identifier, set to the same audience string, byte for byte.
2. **The CLI (`kimmy-cli`) as a public client** allowed the device flow and
   that resource in its `allowed_resources`.

Then prove it end to end:

```sh
export KIMMY_URL=https://kimmydb.example.com
export KIMMY_TOKEN=$(kimmy login)      # device flow: a code and URL on stderr
kimmy whoami                           # federated: true, and your grants
kimmy databases                        # what those grants can actually see
```

If `whoami` shows `"grants":[]`, authentication worked and authorization has
nothing to give — almost always a roles claim no mapping matched, or mappings
that never reached the node. See [Troubleshooting](#troubleshooting).

## The audience is the load-bearing string

`aud` is compared byte for byte, and the same string must appear in **three**
places:

| Place | Set by |
|---|---|
| The node's `audience` | compose/config above |
| The provider's resource registry | one-time admin action |
| Each client's `allowed_resources` | at client registration |

A client asked for a token *for* a resource gets that audience; without an
RFC 8707 `resource` parameter the provider mints its default — usually its own
issuer URL — and every resource trusting that provider shares it. `kimmy login`
reads the right value off the node automatically via the node's protected
resource metadata, so the usual invocation sets nothing extra.

**Write the audience as the public https URL clients reach the node at** when
you can. An https audience *is* an RFC 8707 resource identifier (ADR-071): it
turns on the `/.well-known/oauth-protected-resource` metadata document and the
`resource_metadata` pointer on every 401, which is what lets conformant clients
and MCP servers find their way in unaided. Opaque audiences (`urn:…`,
Entra ID's `api://<guid>`) keep working — they simply publish nothing.

**You can probe the provider without credentials**, which settles "is it
registered?" before any configuration is attempted:

```sh
curl -s -X POST https://auth.example.com/oauth/device_authorization \
  -d 'client_id=kimmy-cli' -d 'scope=openid profile' \
  -d 'resource=https://kimmydb.example.com'
```

A user code means the client exists *and* the resource is allowed — two facts
from one call. `invalid_client`: the client id is wrong or unregistered.
`invalid_target`: the resource string is not registered. A point-in-time probe
answers how it is *now*, never what it was before.

## Provider notes

| Provider | Roles claim | Device flow | Notes |
|---|---|---|---|
| Self-hosted provider that maps a `roles` claim | `roles` (a small fixed set, e.g. `admin\|developer\|user`) | ✅ where implemented | Register the node as a resource server; bound the CLI client's allowed resources to it |
| Microsoft Entra ID | app roles land in `roles`; group IDs in `groups` | ✅ | Access tokens are `typ: JWT`, not `at+jwt` — leave `require_at_jwt` off. Default audiences are `api://<guid>`; a legal opaque audience, publishes no metadata |
| Okta | group names in `groups` | ✅ enable per auth server | Custom claim if you want role names rather than group IDs |
| Keycloak | realm/client roles via a mapper into `roles` | ✅ | Built the mapper or the claim will not exist — a silent zero-grants cause |
| Auth0 | custom claim in a rule/action (`roles` by convention) | ✅ enable per client | Claims must be namespaced; pick the namespace you configure as `KIMMY_OIDC_ROLES_CLAIM` |
| Google | no first-party role concept | limited-input devices only | Map from workspace groups via your own layer, or use opaque audiences deliberately |

Two behaviors are deliberate, not defects: a token whose `typ` is not checked
by default (providers disagree about stamping it; the audience restriction is
the real defense — `require_at_jwt` tightens it when yours stamps `at+jwt`),
and `/v1/auth/refresh` refusing federated principals (minting a local token
from a federated identity would shed the `federated` flag — identity
laundering). There is no revocation path shorter than expiry for a federated
session ([ADR-065](decisions.md)), and the node does not bound expiry itself: a
token of any lifetime verifies, including one with no `iat`
([ADR-112](decisions.md)).

**So the access-token lifetime you configure at your provider is your
revocation window here.** Revoke someone's role at the provider and this node
honours the old claim until their current token expires. Set that lifetime
where every relying party benefits from it — a per-API setting in Auth0, an
access policy on the authorization server in Okta, a token lifetime policy in
Entra ID, a client-level lifespan in Keycloak — rather than expecting the
database to second-guess it. Five to fifteen minutes is the ordinary
recommendation; refresh handles renewal, so a short lifetime costs nobody a
login.

Signing keys rotate at the provider and are picked up here automatically —
a token naming an unknown key triggers one rate-limited refetch, and an
interval re-fetches regardless (`KIMMY_OIDC_REFRESH_INTERVAL_SECS`). Do not pin
a `kid` anywhere.

## The role mapping cookbook

`claim_value` is **the provider's vocabulary**, not this database's — providers
typically enforce a small fixed set at registration (`admin|developer|user` is
a common one). All granularity lives on the KimmyDB side, in either form:

```jsonc
// Inline grants (ADR-066): reviewed in a diff, changed with a restart.
[{"claim_value":"user",
  "grants":[{"db":"*","collection":"*","actions":["read","write","watch","search"]}]}]

// A named ROLE (ADR-073): resolved per request, editable at runtime over
// /v1/roles, shared with local users — one definition serves both paths.
[{"claim_value":"developer","role":"service-developer"}]
```

Rules worth internalizing:

- **Never map `admin`.** An inline mapping naming it refuses at startup, and a
  stored role resolving to it has the action dropped unless
  `allow_federated_admin = true` — a boundary that stays TOML-only so an
  operator reviews it in a diff (ADR-067, ADR-074). Administration stays with a
  local break-glass account: user and role management, backup and the system
  database are `admin`. Collection creation, index management and embedding
  configuration are `ddl`, which maps freely — an agent behind an IdP can
  create the collection it writes to without anyone handing it the server
  (ADR-090).
- **Grants union.** A principal's grants are the union of every matching
  mapping plus direct grants; more than one mapping may contribute.
- **Wildcards mean wildcards.** `{"db":"*"}` matches every database — except
  the system database `__kimmy`, whose `__users` collection holds password
  hashes and token versions: wildcards deliberately never reach it, and only
  the `admin` action or an exact `{db:"__kimmy"}` grant does ([ADR-079]).
  Scope to named databases when a caller needs nothing broader anyway.
- **A mapping nobody matches is not an error.** It produces callers with zero
  grants, which presents as silently empty listings and bare 403s — the CLI
  announces that case since 0.6.0, but prevention beats diagnosis.

## A readable subject: `subject_claim`

A provider's `sub` is stable and opaque — a GUID, an `00u…` string — which is
what makes it a good identity and a bad thing to read in an audit line.
`subject_claim` names a claim whose string value rides beside the identity as
a **display** name:

```sh
export KIMMY_OIDC_SUBJECT_CLAIM=preferred_username   # or email, upn
```

`kimmy whoami` then shows both, and an audit record gains `display=…` when it
differs from `user`:

```json
{ "user": "3f2a9c1e-…", "display": "ada@example.com", "federated": true, "grants": [ … ] }
```

**Display only.** Role mappings, grants, rate limiting and every comparison
the node makes still key on `sub`; a user whose email changes at the provider
keeps exactly the roles they had. That is deliberate and not negotiable
through configuration: an email is mutable, is not unique across providers,
and at some providers is user-editable, so making it the principal would let
a rename merge or split identities silently
([Security](security.md#a-readable-name-that-is-never-an-identity),
[ADR-100](decisions.md)). A token whose claim is missing or not a string is
not refused — `display` simply falls back to `sub`.

Per provider: Entra ID and Keycloak put a login name in `preferred_username`
and an address in `email` (Entra also offers `upn`); Okta uses `email` and
`preferred_username`; Auth0 puts `email` in ID tokens by default and needs an
action to copy it into an access token. Check a real access token — not an ID
token — before choosing, since the claim has to be in the token the node sees.

## Troubleshooting

Work down this list; each step is observable from outside the node.

1. **`kimmy whoami` shows `"grants":[]`.** Authentication succeeded; no
   mapping matched. Decode the token (jwt.io or `jq` on the base64 payload)
   and compare the roles claim against `claim_value`s character by character,
   then confirm the mappings actually reached the node — `docker exec … env`
   or the startup summary line, which prints `(N role mappings)`.
2. **Listings are empty but `whoami` shows grants.** You may be looking at an
   empty namespace — grants filter listings server-side. Check as a local root
   account before assuming a fault.
3. **401 with `invalid_client` at the provider.** The CLI's client id does not
   match a registration (`--client-id` / `KIMMY_OIDC_CLIENT_ID`; there is no
   default).
4. **401 from the node, or a token the node refuses while the provider vouches
   for it.** Audience mismatch, nearly always. Compare the token's `aud` —
   note it arrives as a JSON **array** — against the node's configured
   audience, byte for byte, and remember the same string must sit in the
   provider's registry.
5. **A role you removed at the provider still works.** Not a fault: membership
   is frozen in the access token and there is no introspection, so the change
   lands when that token expires. The wait is the access-token lifetime your
   provider mints. Nothing here shortens it ([ADR-112](decisions.md)) — shorten
   it at the provider if the window matters.
6. **`kimmy whoami` answers with grants or a user that cannot exist here.**
   You are talking to a different server than you think — a leftover dev node
   on `localhost:7878` answers the CLI's default URL. Compare
   `curl $KIMMY_URL/v1/version`'s `node` UUID with what `readyz` on the member
   you meant reports, and export `KIMMY_URL` rather than passing `--url` per
   command.
7. **Everything federated stopped at once after a provider change.** Discovery
   or key rotation. `check-config` reaches the provider and says what it found;
   the node retries in the background rather than refusing to start, so a
   briefly unreachable provider is invisible except in logs.
8. **A federated user needs `/v1/auth/refresh`.** It will keep refusing — that
   is the anti-laundering rule above. Re-authenticate; shorten lifetimes so
   that is cheap.
8. **`kimmy login root` answers 403 or 404.** The node's `auth.local.login`
   is `loopback_only` or `disabled`; the CLI says so under the error. Log in
   from the node's own host (an SSH session, a sidecar), or use the federated
   `kimmy login`. Existing local tokens keep working either way — the mode
   restricts minting, not verifying
   ([Security](security.md#local-login-is-a-mode)).

---

## Where to go next

- [Security](security.md) — the full mechanics: verifier routing, the
  challenge, revocation asymmetry, and the threat model
- [CLI](cli.md) — every command, the token cache, and the zero-grant note
- [Operations](operations.md) — running nodes in containers, health, metrics
- [Decisions](decisions.md) — ADR-064 (verifier routing), ADR-066 (inline
  mappings), ADR-071 (audience = resource identifier), ADR-073 (stored roles),
  ADR-074 (federatable admin), ADR-078 (mappings through the environment),
  ADR-112 (the provider decides how long its tokens live, superseding
  ADR-096), ADR-100 (local login modes and the display name)
