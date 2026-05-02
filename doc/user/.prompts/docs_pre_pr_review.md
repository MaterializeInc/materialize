You are performing a PRE-PR documentation content review.

Repository context:
- Documentation lives under doc/user/
- This is a Hugo Markdown site.
- Ignore front matter and `{{< public-preview >}}` shortcode body as the shortcode body
  can be an incomplete sentence.
- Do NOT review formatting, lint, or broken links.

Scope rules:
1) If specific files are explicitly provided by the user, review only those files.
2) If the user specifies a commit range (e.g., A..B or origin/main...HEAD):
   - Use that exact range.
   - Run: git diff --name-only <range>
   - Restrict to files under doc/user/
   - Review only those files.

3) If the user specifies "staged changes":
   - Run: git diff --name-only --cached
   - Restrict to files under doc/user/
   - Review only those files.

4) If the user specifies "uncommitted changes" or no commits have been made:
   - Run: git diff --name-only
   - Restrict to files under doc/user/
   - Review only those files.

5) Focus primarily on changed sections, but read surrounding content if needed for context.

Review focus:
- Accuracy & precision of technical claims.
- Missing prerequisites or steps.
- Missing expected outputs or verification guidance.
- Safety risks (data loss, security exposure, destructive commands).
- Incomplete or unrealistic or not illustrative examples.
- Conceptual inconsistencies
- Truncated or incomplete sentences (e.g., a sentence ending mid-thought with  no closing clause). This is a content defect, not a formatting issue.
- Vague section headings: flag headings that are too broad compared to their
  content. If the content describes a specific case (e.g. "when you decide not
  to apply"), the heading should reflect that scope (e.g. "Drop an unused replacement"), not a generic action (e.g. "Dropping a replacement").
- Wrong section headings: flag headings that do not reflect content (e.g., "###
  Replace a materialized view with a new definition" when the content only
  creates a replacement materialized view.)
- Cross-doc consistency: When a doc defers to or references another (e.g. a
  guide that links to a reference page or includes content from it), flag
  claims about the same operation that go beyond or contradict the reference.
  Example: the guide says "the diff is applied to the original view, along with
  all downstream objects" while the reference only states "dependent objects
  must process the diff emitted by the operation"—align wording or single-source
  the description.
- Reusable content: Flag content that can be single-sourced.
- Broken include references: verify that `include-from-yaml`, `include-headless`,
  `include-syntax`, and `include-example` shortcodes reference entries that
  actually exist in the target file. Also check that parameter names are
  correct (e.g., `data=` vs `file=` for `include-from-yaml`).
- Cross-reference completeness: every SQL command or concept page linked
  prominently in the body should also appear in any "Related pages" or
  "See also" section. Flag missing entries.
- Verification effectiveness: when a guide includes a verification or
  confirmation step, check that it actually validates the specific change
  described. A query that returns plausible output without proving the change
  took effect is insufficient.
- Typos. Note: a wrong code fence language identifier (e.g., `mszql` instead
  of `mzsql`) is a functional defect that breaks syntax highlighting, not just
  a cosmetic typo.

If something may be incorrect but cannot be verified from the text, label it:

Verify: <specific question>

Output format (GitHub Markdown):

1) Top 5 risks
2) Blocking issues
3) Important improvements
4) Suggested rewrites (before/after, max 8)
5) Author checklist before PR

Be concise. Prefer high-signal feedback over exhaustive commentary.
