//! Roles, grants, and the authorization decision.
//!
//! There is exactly one place that answers "may this principal do this?", and
//! both the HTTP API and the MCP server call it. A second enforcement point is
//! how an MCP tool ends up quietly more permissive than the REST route beside
//! it.

use serde::{Deserialize, Serialize};

/// What a principal wants to do.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    Read,
    Write,
    /// Open a change stream.
    Watch,
    /// Vector and hybrid search.
    Search,
    /// Register a webhook: an endpoint the node pushes change events to.
    ///
    /// Independent of `watch`, deliberately, even though the two carry the same
    /// events. A change stream ends when the client disconnects and dies with
    /// the token that opened it; a webhook keeps sending to an address the
    /// grant never named, long after that token expires. Handing out an egress
    /// path is a different act from being allowed to read, so it is granted
    /// separately rather than arriving bundled with reading.
    Webhook,
    /// Create/drop collections, manage indexes and users.
    Admin,
}

impl Action {
    /// Actions implied by holding this one.
    ///
    /// `Admin` implies everything, so an administrator does not need every
    /// action listed explicitly; `Write` implies `Read`, because an update has
    /// to read the document it modifies.
    fn implied_by(self) -> &'static [Action] {
        match self {
            Action::Read => &[Action::Read, Action::Write, Action::Admin],
            Action::Write => &[Action::Write, Action::Admin],
            Action::Watch => &[Action::Watch, Action::Admin],
            Action::Search => &[Action::Search, Action::Read, Action::Write, Action::Admin],
            // Only `Admin` implies it. `Watch` deliberately does not: see the
            // variant's documentation.
            Action::Webhook => &[Action::Webhook, Action::Admin],
            Action::Admin => &[Action::Admin],
        }
    }
}

/// One permission: a set of actions on a set of collections.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grant {
    /// Database name, or `*` for any.
    pub db: String,
    /// Collection name pattern. Supports a single trailing `*`, or `*` alone.
    #[serde(default = "star")]
    pub collection: String,
    pub actions: Vec<Action>,
}

fn star() -> String {
    "*".to_string()
}

impl Grant {
    pub fn new(db: impl Into<String>, collection: impl Into<String>, actions: Vec<Action>) -> Self {
        Self { db: db.into(), collection: collection.into(), actions }
    }

    /// Full access to everything.
    pub fn superuser() -> Self {
        Self::new("*", "*", vec![Action::Admin])
    }

    fn covers(&self, db: &str, collection: Option<&str>, action: Action) -> bool {
        if !pattern_matches(&self.db, db) {
            return false;
        }
        // A database-wide request (no collection named) is only satisfied by a
        // grant that spans the whole database.
        match collection {
            Some(name) => {
                if !pattern_matches(&self.collection, name) {
                    return false;
                }
            }
            None => {
                if self.collection != "*" {
                    return false;
                }
            }
        }
        let accepted = action.implied_by();
        self.actions.iter().any(|held| accepted.contains(held))
    }
}

/// Match a name against a pattern supporting one trailing `*`.
///
/// Deliberately not a full glob: `orders*` and `*` cover the real cases, and a
/// richer syntax invites patterns whose blast radius is hard to eyeball in an
/// audit.
fn pattern_matches(pattern: &str, name: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => name.starts_with(prefix),
        None => pattern == name,
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Role {
    pub name: String,
    #[serde(default)]
    pub grants: Vec<Grant>,
}

/// An authenticated caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Principal {
    pub user: String,
    pub grants: Vec<Grant>,
    /// Set when the server runs with `--insecure-no-auth`.
    pub unauthenticated: bool,
    /// Set when the identity came from the external OIDC provider rather than
    /// from this cluster's own user store.
    ///
    /// Flagged for the same reason `unauthenticated` is: audit output must be
    /// able to tell these apart. "ada did this" and "somebody the identity
    /// provider called ada did this" are different claims, and only one of them
    /// rests on a password this cluster stores. It also decides revocation —
    /// there is no local record to carry a token version, so the session check
    /// has nothing to check (ADR-065).
    pub federated: bool,
    /// The user's token version when this principal was issued a token.
    ///
    /// Carried so a request can be checked against the version the user
    /// currently has: a mismatch means the token was invalidated (ADR-052).
    pub token_version: u64,
    /// Stored roles this principal holds whose grants are **not yet resolved**.
    ///
    /// Only ever populated on the federated path, and only between the verifier
    /// and the extractor. A local user's roles are resolved when it
    /// authenticates, because its grants are embedded in the token it is
    /// issued; a federated principal has no such moment, so the names travel
    /// this far and are resolved against the store on every request (ADR-073).
    ///
    /// Empty by the time any authorization decision is made — the extractor
    /// resolves them into `grants` — so `can` deliberately does not consult it.
    /// It is kept afterwards for the audit record, which reports the roles
    /// *held*.
    pub roles: Vec<String>,
}

impl Principal {
    pub fn new(user: impl Into<String>, grants: Vec<Grant>) -> Self {
        Self {
            user: user.into(),
            grants,
            unauthenticated: false,
            federated: false,
            token_version: 0,
            roles: Vec::new(),
        }
    }

    /// The principal an external identity provider's token authorizes.
    ///
    /// No token version, because there is no local record to hold one. What
    /// ends a federated session is the provider: a short token lifetime, and
    /// the provider refusing to mint the next one (ADR-065).
    pub fn federated(user: impl Into<String>, grants: Vec<Grant>) -> Self {
        Self {
            user: user.into(),
            grants,
            unauthenticated: false,
            federated: true,
            token_version: 0,
            roles: Vec::new(),
        }
    }

    /// The same principal, at a stated token version.
    pub fn at_version(mut self, token_version: u64) -> Self {
        self.token_version = token_version;
        self
    }

    /// The same principal, holding stored roles that are not yet resolved.
    ///
    /// A builder rather than a parameter on [`Self::federated`] so that every
    /// existing construction keeps compiling unchanged — `verify` has a great
    /// many call sites, and widening its result would be churn in return for
    /// nothing.
    pub fn with_roles(mut self, roles: Vec<String>) -> Self {
        self.roles = roles;
        self
    }

    /// Add resolved grants, and forget nothing about where they came from.
    ///
    /// The union is additive, always (ADR-073): Kubernetes RBAC is purely
    /// additive and Postgres unions privileges across role membership, and more
    /// to the point it is the only rule that needs no rewrite of an existing
    /// record, since a principal holding no roles gets exactly what it got
    /// before.
    pub fn extend_grants(&mut self, grants: impl IntoIterator<Item = Grant>) {
        self.grants.extend(grants);
    }

    pub fn superuser(user: impl Into<String>) -> Self {
        Self::new(user, vec![Grant::superuser()])
    }

    /// The principal used when authentication is disabled.
    ///
    /// Explicitly flagged rather than being an ordinary superuser, so that
    /// audit output can tell "root did this" from "auth was off".
    pub fn insecure_root() -> Self {
        Self {
            user: "insecure-no-auth".into(),
            grants: vec![Grant::superuser()],
            unauthenticated: true,
            federated: false,
            token_version: 0,
            roles: Vec::new(),
        }
    }

    /// May this principal perform `action` on `db.collection`?
    ///
    /// Pass `None` for the collection to ask about a database-wide operation.
    pub fn can(&self, action: Action, db: &str, collection: Option<&str>) -> bool {
        self.grants.iter().any(|g| g.covers(db, collection, action))
    }

    /// Collections in `db` this principal may act on, filtered from a list.
    ///
    /// Listing must not leak the existence of collections the caller cannot
    /// see, so enumeration goes through the same check as access.
    pub fn visible<'a>(
        &self,
        action: Action,
        db: &str,
        names: impl IntoIterator<Item = &'a str>,
    ) -> Vec<&'a str> {
        names.into_iter().filter(|n| self.can(action, db, Some(n))).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn analyst() -> Principal {
        Principal::new(
            "analyst",
            vec![Grant::new("sales", "orders*", vec![Action::Read, Action::Watch])],
        )
    }

    #[test]
    fn a_grant_permits_its_own_actions() {
        let p = analyst();
        assert!(p.can(Action::Read, "sales", Some("orders")));
        assert!(p.can(Action::Watch, "sales", Some("orders")));
    }

    #[test]
    fn a_grant_denies_actions_it_does_not_list() {
        let p = analyst();
        assert!(!p.can(Action::Write, "sales", Some("orders")));
        assert!(!p.can(Action::Admin, "sales", Some("orders")));
    }

    #[test]
    fn grants_are_scoped_to_their_database() {
        let p = analyst();
        assert!(!p.can(Action::Read, "hr", Some("orders")));
    }

    #[test]
    fn trailing_star_matches_a_prefix() {
        let p = analyst();
        assert!(p.can(Action::Read, "sales", Some("orders_2024")));
        assert!(p.can(Action::Read, "sales", Some("orders")));
        assert!(!p.can(Action::Read, "sales", Some("invoices")));
    }

    #[test]
    fn a_prefix_pattern_does_not_match_a_shorter_name() {
        let p = Principal::new("x", vec![Grant::new("db", "orders*", vec![Action::Read])]);
        assert!(!p.can(Action::Read, "db", Some("order")));
    }

    #[test]
    fn write_implies_read() {
        // An update has to read the document it modifies, so requiring both to
        // be granted separately would make every writer role wrong by default.
        let p = Principal::new("w", vec![Grant::new("db", "*", vec![Action::Write])]);
        assert!(p.can(Action::Read, "db", Some("c")));
        assert!(p.can(Action::Write, "db", Some("c")));
        // ...but write does not imply watching or administration.
        assert!(!p.can(Action::Watch, "db", Some("c")));
        assert!(!p.can(Action::Admin, "db", Some("c")));
    }

    #[test]
    fn admin_implies_everything() {
        let p = Principal::new("a", vec![Grant::new("db", "*", vec![Action::Admin])]);
        for action in [Action::Read, Action::Write, Action::Watch, Action::Search, Action::Admin] {
            assert!(p.can(action, "db", Some("c")), "admin should imply {action:?}");
        }
    }

    #[test]
    fn read_does_not_imply_write() {
        let p = analyst();
        assert!(!p.can(Action::Write, "sales", Some("orders")));
    }

    #[test]
    fn a_superuser_reaches_every_database() {
        let p = Principal::superuser("root");
        assert!(p.can(Action::Admin, "anything", Some("at-all")));
        assert!(p.can(Action::Write, "other", None));
    }

    #[test]
    fn database_wide_requests_need_a_database_wide_grant() {
        // A grant limited to one collection must not authorize an operation
        // that spans the database, such as dropping it.
        let scoped = Principal::new("s", vec![Grant::new("db", "orders", vec![Action::Admin])]);
        assert!(scoped.can(Action::Admin, "db", Some("orders")));
        assert!(!scoped.can(Action::Admin, "db", None));

        let wide = Principal::new("w", vec![Grant::new("db", "*", vec![Action::Admin])]);
        assert!(wide.can(Action::Admin, "db", None));
    }

    #[test]
    fn several_grants_combine() {
        let p = Principal::new(
            "multi",
            vec![
                Grant::new("a", "*", vec![Action::Read]),
                Grant::new("b", "*", vec![Action::Write]),
            ],
        );
        assert!(p.can(Action::Read, "a", Some("x")));
        assert!(!p.can(Action::Write, "a", Some("x")));
        assert!(p.can(Action::Write, "b", Some("x")));
    }

    #[test]
    fn a_principal_with_no_grants_can_do_nothing() {
        let p = Principal::new("nobody", vec![]);
        assert!(!p.can(Action::Read, "db", Some("c")));
        assert!(!p.can(Action::Read, "db", None));
    }

    #[test]
    fn listing_hides_collections_the_caller_cannot_read() {
        // Enumeration must not leak the existence of what access would deny.
        let p = analyst();
        let all = ["orders", "orders_archive", "salaries"];
        assert_eq!(p.visible(Action::Read, "sales", all), vec!["orders", "orders_archive"]);
    }

    #[test]
    fn the_no_auth_principal_is_distinguishable_from_root() {
        let p = Principal::insecure_root();
        assert!(p.can(Action::Admin, "any", Some("thing")));
        assert!(p.unauthenticated, "audit output must be able to tell these apart");
        assert!(!Principal::superuser("root").unauthenticated);
    }

    #[test]
    fn a_federated_principal_is_distinguishable_from_a_local_one_of_the_same_name() {
        // The three origins are three different claims, and an audit reader has
        // to be able to tell which one it is looking at. A name is not enough:
        // the identity provider is free to call somebody "root".
        let federated =
            Principal::federated("root", vec![Grant::new("db", "*", vec![Action::Read])]);
        assert!(federated.federated);
        assert!(!federated.unauthenticated);

        let local = Principal::superuser("root");
        assert!(!local.federated);
        assert!(!local.unauthenticated);

        assert!(!Principal::insecure_root().federated);
    }

    #[test]
    fn roles_round_trip_through_json() {
        let role = Role {
            name: "analyst".into(),
            grants: vec![Grant::new("sales", "orders*", vec![Action::Read, Action::Watch])],
        };
        let text = serde_json::to_string(&role).unwrap();
        assert_eq!(serde_json::from_str::<Role>(&text).unwrap(), role);
    }

    #[test]
    fn a_grant_without_a_collection_defaults_to_the_whole_database() {
        let g: Grant = serde_json::from_str(r#"{"db":"sales","actions":["read"]}"#).unwrap();
        assert_eq!(g.collection, "*");
    }
}
