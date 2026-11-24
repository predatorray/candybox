# Security

Use this file to review how a service-based design protects data and limits damage. Services increase the attack surface (more endpoints, more network traffic) but also make it possible to isolate sensitive parts and limit what any one breach can reach.

## Principles

- **Least privilege.** Each service, person, and credential gets only the access it needs, for as short a time as practical. A compromised service should be able to reach very little.
- **Defense in depth.** Several independent layers of protection. If one fails, others still hold. Splitting a system into services makes it possible to put stronger controls around the sensitive parts only.
- **Automation.** With many services, manual patching, certificate renewal, and secret rotation will be missed. Automate them.
- **Security in the delivery process.** Dependency scanning, image scanning, and static checks run in every pipeline. Teams own the security of their services, with specialists helping rather than gatekeeping at the end.

## Cover the whole lifecycle

A design should say how it will:
- **Identify** what needs protecting (data classification, threat model for the main flows).
- **Protect** it (the controls below).
- **Detect** problems (security-relevant logs, alerts on unusual access).
- **Respond** to an incident (who is called, how a credential is revoked quickly).
- **Recover** (restore from backups, rebuild hosts from scratch).

Designs often cover only "protect".

## Basics that cause most breaches

- **Credentials.** Keep secrets in a secret manager, never in code, images, or plain config. Scope each credential narrowly. Rotate automatically. Scan repos for leaked keys. Be able to revoke any credential quickly.
- **Patching.** OS, container base images, language runtimes, and third-party libraries. Know what versions run where, and rebuild and redeploy regularly.
- **Backups.** Automated, stored separately from production (so an attacker who gets into production can't delete them), and restore-tested.
- **Rebuild.** Be able to recreate any environment from code. After a compromise, rebuilding is safer than cleaning.

## Trust between services

- **Implicit trust:** anything inside the network perimeter is trusted. Once an attacker is inside, they can call anything.
- **Zero trust:** assume the network is already compromised. Every call is authenticated and authorized, and traffic is encrypted, regardless of where it comes from.

This is a range, not a binary. Apply stricter controls where data is more sensitive. Services handling payments or personal data should not accept unauthenticated calls just because they come from inside the cluster.

## Protecting data

**In transit.** Encrypt service-to-service traffic (TLS; mutual TLS also authenticates both sides). Make sure data can't be read or changed in transit, and that each side knows who it is talking to. A service mesh can make mTLS easier to roll out.

**At rest.** Encrypt sensitive stores, with keys managed separately from the data. Even better:
- Collect less. Data you never stored can't leak. Question each personal field: is it needed, and for how long?
- Isolate sensitive data in a small number of services, so fewer systems fall in scope for audits and fewer places need the strongest controls.
- Don't copy sensitive data into logs, events, caches, or analytics feeds without the same controls.

## Authentication and authorization

**People.** Use single sign-on with a standard identity provider. A gateway at the edge can handle login and token checks so each service doesn't implement it.

**Services.** Each service has its own identity (mTLS certificate, or a token from a central issuer). Services check which caller is making a request, not just that it came from inside.

**Where authorization decisions belong.** A gateway can check coarse rules ("is this a logged-in user?"). Fine-grained rules ("can this user cancel this booking?") belong in the service that owns the data, because only it understands the domain rules. A central service that makes every business authorization decision becomes a bottleneck and couples all services to it.

**The confused deputy problem.** If a downstream service trusts any request from an upstream service, a user who can trick the upstream can act on data they shouldn't see. Example: the web BFF asks Booking for booking 123; Booking returns it because the BFF is trusted, but booking 123 belongs to someone else. Fix: pass the end user's identity and claims downstream (for example, in a signed token), and have the owning service check that this user may access this record.

**Signed tokens (such as JWTs).** Let services verify the caller's identity and claims locally, without calling a central auth service on every request. Points to check:
- Short expiry, because revocation is hard.
- Minimal claims; don't put sensitive data in tokens.
- Scope tokens to an audience so a token for one service can't be replayed against another.
- Key rotation for signing keys.

## Review checklist

- [ ] Sensitive data is classified, and the design minimizes and isolates it.
- [ ] Secrets are in a secret manager, scoped, rotated, and revocable.
- [ ] Patching and image rebuilds are automated.
- [ ] Backups are separate from production and restore-tested.
- [ ] Service-to-service traffic is authenticated and encrypted where data warrants.
- [ ] Each service authorizes requests on behalf of the end user, not just the calling service.
- [ ] Fine-grained authorization lives in the owning service.
- [ ] Tokens are short-lived, scoped, and minimal.
- [ ] There is a plan for detect, respond, and recover, not just protect.

Related: [communication.md](communication.md) for gateways and meshes, [observability.md](observability.md) for logging hygiene.
