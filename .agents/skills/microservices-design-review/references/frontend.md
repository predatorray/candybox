# User interfaces and client-facing aggregation

Use this file when the design includes web or mobile clients, an API layer for them, or questions about who owns the UI. A common failure is to split the backend into services owned by product teams but keep one frontend owned by a separate team, so every feature still needs a handoff.

## Ownership

**Separate frontend team.** One team owns the whole UI; backend teams own services. Every feature needs work from both, with a queue in between. This often comes from scarce UI specialists or a wish for consistent design.

**End-to-end teams.** Each team owns a slice of the product across UI and backend (for example, the booking flow from page to database). Features ship within one team. To make this work:
- Spread specialists (UI, UX, accessibility) across teams, or have a small group that supports several teams.
- Keep the UI consistent with a shared design system and component library, owned by someone, that teams pull in.
- Accept that some technical work (build tooling, performance budgets) needs cross-team coordination.

Recommend end-to-end ownership where the organization can support it, and say what's needed for it to work.

## Splitting the frontend

**Monolithic frontend.** One UI codebase calling many services. Fine when one team owns it, or the UI is small. Becomes a bottleneck when many teams need to change it.

**Micro frontends.** The UI is split into parts that different teams build and deploy independently.
- **By page:** each team owns whole pages or routes. Simple, works with plain links between pages. Good default for websites.
- **By widget:** a page is composed of components owned by different teams (a search box, a recommendations panel). More flexible, more complex: shared dependencies, communication between widgets, load performance.

Things to check: shared state between parts is minimal, parts communicate through events or URL state rather than reaching into each other, bundle size and duplicated libraries are watched, and the overall look stays consistent.

## Aggregating backend calls for clients

Clients often need data from several services for one screen, and mobile clients in particular want fewer, smaller responses.

**Central aggregating gateway.** One shared API layer combines and filters calls for all clients.
- Works when one team owns both the gateway and all UIs.
- Becomes a problem when several teams need to change it: a shared component everyone has to queue for. It also tends to collect logic for every client type, and those needs often conflict.

**Backend for frontend (BFF).** A separate aggregation service per type of experience (one for the mobile app, one for the web app, one for partner integrations), owned by the team that owns that UI.
- Each client gets an API shaped for its needs, and its team can change it freely.
- How many: one per distinct user experience. Mobile iOS and Android often share one if they show the same things. Don't create one per device model.
- Keep BFFs thin: aggregation, filtering, and shaping. Domain logic belongs in the underlying services. If several BFFs duplicate the same logic, that logic probably belongs in a service, or in a shared library if it is presentation-only.

**GraphQL.** Lets clients request exactly the fields they need from a schema that resolves across services. Reduces the need for client-specific BFFs. Check for: expensive queries that fan out to many services (set depth and cost limits), caching (harder than with plain GET requests), and ownership of the shared schema. Using it for reads is common; using it for complex writes needs more care.

A mixed approach is normal: a BFF for the mobile app, page-based micro frontends for the website, and GraphQL behind one of them.

## Review checklist

- [ ] The teams that own backend capabilities can also change the UI for them, or the handoff cost is accepted knowingly.
- [ ] If the UI is split, the split is by page unless there is a reason for widgets.
- [ ] UI consistency has an owner (design system, component library).
- [ ] Aggregation layers are owned by the UI's team, not a central team.
- [ ] Aggregation layers contain no domain rules.
- [ ] GraphQL, if used, has query cost limits and a schema owner.
- [ ] Mobile clients' payload and round-trip needs are addressed.

Related: [communication.md](communication.md) for gateways, [organization.md](organization.md) for team structure.
