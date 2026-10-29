# Communicating architecture

Covers: what to look for when reviewing architecture diagrams, how to present findings so they get acted on, and how to push back on stakeholders, other architects and developers. Use it when the input is mostly diagrams, or when the user needs help getting a review or a decision accepted.

## Reviewing diagrams

A diagram is often the main artifact in a design review. Check it the way you would check code:

- **Context first.** Before a detailed view of one part, there should be an overview showing where that part sits. A zoomed-in diagram with no anchor leaves readers guessing what is outside the frame.
- **Levels.** A useful set: system context (users and external systems), containers (deployable units and data stores), components inside a container. Mixing levels on one page (a database next to a class name next to a whole external SaaS) confuses more than it shows.
- **Everything labeled.** Every box and every line has a name. Unlabeled arrows are the most common gap.
- **Line meaning stated.** Arrow direction shows who calls whom or which way data flows. A common convention is solid lines for synchronous calls and dashed lines for asynchronous messages. Whatever the convention, the diagram needs a key if anything is ambiguous.
- **Shapes and color used consistently.** One shape for deployable units, another for containment, color to distinguish different services rather than for decoration.
- **Data stores shown with owners.** Which service writes to which store is often the most important coupling in the picture and the one most often left off.
- **External limits annotated.** Rate limits, SLAs, and quotas of third-party dependencies belong on the diagram, near the arrow that hits them.

A misleading diagram is worse than none. If the diagram and the text disagree (the text says async, the arrow is solid), raise it as a finding.

Early in design, sketches are better than polished diagrams. People get attached to artifacts in proportion to the time spent making them, and a two-hour diagram is harder to throw away than a whiteboard photo. Polish after the design settles.

## Presenting findings

- Lead with the verdict and the top risks, then details. Busy readers stop early.
- Tie each finding to a quality attribute or a business goal the audience cares about ("this puts the April registration peak at risk"), not to a principle ("this violates loose coupling").
- Put numbers in terms people feel. Downtime in minutes per month, not nines. Latency in seconds the user waits, not milliseconds per hop.
- Offer options with costs, and say which you recommend.
- For a live presentation, the slides carry the picture and the speaker carries the argument; do not put every word on the slide. For a document that will be read alone, include everything the reader needs.

## Pushing back

**Business stakeholders.** Listen for what the strong language reveals. "It must never go down" says availability is the fear; "I needed this yesterday" says time to market. Do your homework before the conversation: what does the demand cost, and what does the realistic alternative look like? Narrow the demand to the part of the system that needs it ("five nines for the checkout API, three for reporting"). Raise cost and time last, after agreeing on what is important; opening with "that's too expensive" ends the conversation before it starts.

**Other architects.** Prefer evidence over argument. If two people disagree on whether messaging or REST is faster in this environment, measure both on the real infrastructure. A short proof of concept settles more debates than a long thread.

**Developers.** Explain the reason behind each constraint. People follow rules they understand and route around rules they do not. When a developer wants to change a standard, ask them to make the case including the business benefit; often they will reach the right answer themselves. Involve the people who will implement a decision before it is final; they will find problems you cannot see from the diagram.

## Red flags

- Diagrams with unlabeled arrows or no key.
- No view showing data stores and their owners.
- A detailed diagram with no overview.
- Diagram and text disagree about sync versus async.
- Findings framed as principle violations with no stated consequence.
- Availability demands discussed only as nines.
