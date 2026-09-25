# {{PROJECT_NAME}}: Engineering Principles

The manual behind the summary in `CLAUDE.md`. Each principle carries its
**failure mode**, the way it goes wrong when applied mechanically, because
principles recited without judgement produce worse code than none.

## 1. DRY: one authoritative representation of each piece of knowledge

- **Do.** a business rule, schema, constant, protocol string, or config
  default is defined once and referenced everywhere (shared constants module,
  generated code, single schema file).
- **Do.** docs point at the source of truth instead of restating it.
- **Don't.** merge two code paths just because they *look* alike today. If
  they change for different reasons they are different knowledge.
- **Rule of three.** tolerate a second copy; extract on the third, once the
  real shape of the abstraction is visible.
- **Failure mode.** the "wrong abstraction": a shared helper sprouting
  boolean flags for each caller. Inline it back and re-extract.

## 2. SOLID: apply at real seams, not everywhere

| Principle | Means here | Failure mode |
|---|---|---|
| **S**ingle responsibility | a module/function has one reason to change; name it and the name should not need "and" | splitting into so many tiny files nobody can follow the flow |
| **O**pen/closed | extend via new implementations/registrations, not by editing a growing switch in the core | plugin systems for things with one implementation |
| **L**iskov substitution | any implementation of an interface honours its contract (errors, bounds, ordering) | subclasses that throw "not supported" |
| **I**nterface segregation | small, role-focused interfaces; callers depend on what they use | one interface per method |
| **D**ependency inversion | high-level policy depends on abstractions at boundaries (storage, network, clock) so it can be tested | DI containers and factories for pure functions |

## 3. KISS & YAGNI

- Build for the requirement in front of you. Configuration, generality and
  extension points are added when the second real use appears.
- Prefer boring, explicit code over clever code. Optimise for the reader.
- Delete dead code; version control remembers it.

## 4. Boundaries & errors

- Validate at the edge (input, IO, network); trust internal invariants.
- Errors are values across module boundaries (Result/Status/typed errors);
  never use exceptions or panics for expected control flow.
- Assertions are for programmer errors: preconditions and postconditions,
  positive and negative space. No side effects inside an assert.
- Every loop, queue, buffer and retry has an explicit bound. Fail loudly on
  overflow; never grow silently.

## 5. Structure

- Functions short enough to read in one screen (~70 lines guide).
  Files split along feature seams before they become dumping grounds.
- Dependency direction is documented in `PROJECT_MAP.md`; cycles are bugs.
- Keep side effects at the edges; keep the core pure where practical.
- Name things for what they mean in the domain, not how they're implemented.

## 6. Completeness

- A change ships with tests. Bug fixes ship with a test that failed before.
- Unfinished capability returns an explicit "not implemented", never a
  silent stub that pretends to succeed.
- Zero new warnings; fix, don't suppress.
- Update `PROJECT_MAP.md` / the area `CLAUDE.md` in the same change when structure
  or behaviour they describe changes.

## 7. Dependencies

- Prefer what the project already uses over adding a new dependency.
- A new dependency needs a reason written in the PR: what it replaces, its
  maintenance status, its footprint.

## 8. Performance (when it matters here)

- Measure before and after; record the baseline. No regressions without a
  stated trade-off.
- Bound tail latency, not just the mean, for anything user- or
  latency-facing.

## 9. Pre-commit checklist

- [ ] Build, tests, lint green locally
- [ ] New/changed behaviour has a test
- [ ] No duplicated knowledge introduced (constants, schemas, rules)
- [ ] PROJECT_MAP.md / area CLAUDE.md updated if structure changed
- [ ] `kb.py check` passes
