# {{PROJECT_NAME}}: Documentation Style & Structure

For documentation / knowledge / content projects. The same ideas as the code
principles, translated.

## Structure

- **One topic, one home (DRY).** Each fact lives in exactly one page; other
  pages link to it. Duplicated explanations drift and contradict.
- **Single responsibility per page.** A page answers one question or covers
  one concept. If its title needs "and", split it.
- **Diátaxis split** where it helps: tutorials (learn), how-tos (do),
  reference (look up), explanation (understand). Don't mix them in a page.
- **Stable anchors.** Headings are link targets; renaming one is a breaking
  change, so search for inbound links (`kb.py search`) first.

## Writing

- Lead with the answer; context after.
- Concrete over abstract: an example beats a paragraph.
- Define terms once in the glossary (PROJECT_MAP.md → Key concepts) and link.
- Mark uncertainty ("unverified:") and dates on time-sensitive claims.
- Corrections are visible: `[CORRECTED YYYY-MM-DD: …]`.

## Areas

Each section/folder has its own `CLAUDE.md` describing: audience, scope (and
what is out of scope), source-of-truth pages, naming conventions, and
review rules.

## Checklist

- [ ] Facts not duplicated elsewhere (search first)
- [ ] Links resolve (`kb.py check`)
- [ ] PROJECT_MAP.md updated if a section was added/moved
