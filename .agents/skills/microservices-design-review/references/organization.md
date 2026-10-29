# Teams, ownership, and architecture governance

Use this file when the design's success depends on how teams are organized, who owns what, and how cross-cutting decisions are made. Many microservice designs fail for organizational reasons even when the technical design is sound.

## Systems mirror the teams that build them

The structure of a system tends to follow the communication structure of the organization that builds it. Two teams that rarely talk will build parts with a clear interface between them; one team will build tightly connected code. Use this in both directions:
- When reviewing a design, compare service boundaries with team boundaries. A service owned by three teams will be hard to change. Three services owned by one tiny team may be more overhead than they need.
- When designing, it can make sense to shape teams around the architecture you want, so that the structure holds over time.

## Team design

- **Small teams.** Roughly the size that can share context easily, often five to ten people. Larger teams split informally anyway.
- **Aligned with a stream of work.** A team owns a business area end to end (build, run, on-call) for a long time, not a technical layer and not a project that ends.
- **Autonomy needs tools.** A team that must ask another team to provision a database, open a firewall rule, or deploy is not autonomous. Self-service is a prerequisite.

## Ownership models

**Strong ownership.** Each service has one owning team. Others ask, or submit changes that the owner reviews. This is the default for services that need to change independently.

**Collective ownership.** Every team is free to edit every service. Works for small organizations with strong shared standards. At scale, it tends to erode boundaries, because nobody feels responsible for keeping a service coherent.

The review question is simple: does every service, shared library, topic, and data store have a named owning team? Things without owners decay, and nobody gets paged when they break.

## Shared services and how they go wrong

A service used and changed by many teams becomes a delivery bottleneck. Common reasons and remedies:
- **It's too hard to split.** Consider internal open source: one owning team keeps core committers, and other teams contribute changes through reviewed pull requests. The owners set standards and keep it coherent.
- **Changes cut across many services.** Often a sign the boundaries are wrong. Revisit them.
- **One team can't keep up with requests.** Add capacity, or let consumers contribute as above, or split the service so others can own parts.

Watch for **orphaned services**: something still running in production whose team was reorganized away. Every review of a large system should ask whether such services exist and who now owns them.

## Supporting teams

- **Enabling teams** help product teams build skills in a specific area (security, testing, observability) and then step back. They advise and pair; they don't become a gate.
- **Communities of practice** let people in the same discipline across teams share approaches.
- **Platform teams** provide self-service tools: deployment, logging, metrics, service templates. Treat the platform as a product whose users are internal teams.

**Paved road.** The platform should make the recommended way the easiest way, not the only allowed way. If teams can opt out when they have a reason, the platform team stays focused on making it worth using. Mandated platforms that are hard to use lead to workarounds.

## Distributed teams

Teams in different time zones should own services with little day-to-day coupling between them. Splitting one service between two locations usually slows both.

## Guiding the architecture over time

The architecture will change. Plans should assume that and make change safe, instead of trying to fix the design forever.

**From goals to practices.** A useful structure for technical direction:
1. Strategic goals: what the organization is trying to achieve (enter new markets, let partners integrate, cut time to market).
2. Principles: rules that support the goals ("services expose capabilities through public APIs", "each team can deploy without coordination"). Keep the list short, ten or fewer.
3. Practices: concrete ways the principles are carried out ("all APIs have an OpenAPI spec checked in CI", "every service has a dashboard and runbook"). Practices change more often than principles.

Check that principles trace back to goals, and that practices exist for each principle. Principles nobody can turn into practice are slogans.

**Required standards.** A few things every service must do, so the whole system stays operable:
- Emit logs, metrics, and health checks in the agreed format.
- Use the agreed interface styles and schema conventions.
- Protect itself and others: timeouts, sensible retry behavior, authentication.

Keep this list short; a long list of requirements drowns the few that matter.

**Making the right way easy.**
- Exemplar services that show the standards working in real code.
- A service template or starter that comes with logging, metrics, health checks, auth, and a pipeline already wired up. Teams should be able to update it over time, and it shouldn't hide so much that teams don't understand their own service.
- Automated checks in pipelines for the required standards.

**Technical debt and exceptions.** Track debt and deliberate exceptions to standards in a visible place, with reasons. Many exceptions to the same rule mean the rule is wrong.

**Decision records.** Write down significant architecture decisions with context, options considered, and why the choice was made, so later teams know what was intended.

**The architect's role.** In an organization of autonomous teams, the people responsible for technical direction work across teams: setting a small number of principles, spending time with teams, reviewing designs, and spotting problems between teams' boundaries. Decisions inside a service belong to its team. Someone still needs time to look across the whole system, especially as the number of teams grows.

## Review checklist

- [ ] Every service and shared component has one named owning team.
- [ ] Service boundaries roughly match team boundaries.
- [ ] Teams can provision, deploy, and observe their services without filing requests.
- [ ] Shared services have a contribution model.
- [ ] No orphaned services.
- [ ] Platform tools are a paved road, not a mandate.
- [ ] Principles are few and trace to goals; practices make them real.
- [ ] Required cross-cutting standards are short and enforced automatically.
- [ ] Exceptions and debt are tracked.

Related: [frontend.md](frontend.md) for UI ownership, [fundamentals.md](fundamentals.md) for whether the organization is ready.
