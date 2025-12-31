#!/usr/bin/env python3
"""
Skill Upgrade Script: Transform Materialize docs for LLM accessibility.

This script processes markdown files in .claude/skills/materialize-docs/ to:
1. Resolve Hugo shortcodes
2. Add YAML front matter metadata
3. Add progressive-disclosure preambles
4. Convert tables to bulleted lists
5. Add section summaries
6. Normalize code blocks
7. Rewrite links for local use
"""

import os
import re
import yaml
from pathlib import Path
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass, field

# Configuration
SKILL_DIR = Path(".claude/skills/materialize-docs")
SHARED_CONTENT_DIR = Path("doc/user/shared-content")
MAX_TOKENS_THRESHOLD = 10000  # ~40,000 characters

@dataclass
class ProcessingStats:
    """Track processing statistics."""
    files_processed: int = 0
    files_skipped: int = 0
    shortcodes_resolved: int = 0
    tables_converted: int = 0
    metadata_added: int = 0
    errors: List[str] = field(default_factory=list)


def estimate_tokens(text: str) -> int:
    """Rough token estimation: ~4 chars per token."""
    return len(text) // 4


def resolve_include_md(match: re.Match, base_path: Path) -> str:
    """Resolve {{< include-md file="..." >}} shortcodes."""
    file_attr = match.group(1)

    # Try different path resolutions
    possible_paths = [
        SHARED_CONTENT_DIR / file_attr,
        SHARED_CONTENT_DIR / file_attr.replace("shared-content/", ""),
        Path("doc/user/content") / file_attr,
    ]

    for path in possible_paths:
        if path.exists():
            content = path.read_text()
            # Recursively resolve shortcodes in included content
            return resolve_shortcodes(content, path.parent)

    # If file not found, leave a placeholder
    return f"<!-- Include not found: {file_attr} -->"


def resolve_tabs(content: str) -> str:
    """Convert {{< tabs >}}...{{< /tabs >}} to sequential H4 sections."""
    # Pattern to match tab blocks
    tabs_pattern = re.compile(
        r'\{\{<\s*tabs\s*>\}\}(.*?)\{\{<\s*/tabs\s*>\}\}',
        re.DOTALL
    )

    def process_tabs_block(match: re.Match) -> str:
        tabs_content = match.group(1)

        # Extract individual tabs
        tab_pattern = re.compile(
            r'\{\{<\s*tab\s+"([^"]+)"\s*>\}\}(.*?)(?=\{\{<\s*(?:/tab|tab\s+")|$)',
            re.DOTALL
        )

        result_parts = []
        for tab_match in tab_pattern.finditer(tabs_content):
            tab_name = tab_match.group(1)
            tab_content = tab_match.group(2).strip()
            # Remove closing tab tags
            tab_content = re.sub(r'\{\{<?\s*/?\s*tab\s*>?\}\}', '', tab_content).strip()
            result_parts.append(f"#### {tab_name}\n\n{tab_content}")

        return "\n\n".join(result_parts)

    return tabs_pattern.sub(process_tabs_block, content)


def resolve_admonitions(content: str) -> str:
    """Convert admonition shortcodes to blockquotes."""
    # Warning - both {{< >}} and {{% %}} styles
    content = re.sub(
        r'\{\{<\s*warning\s*>\}\}(.*?)\{\{<?\s*/?\s*warning\s*>?\}\}',
        r'> **Warning:** \1',
        content, flags=re.DOTALL
    )
    content = re.sub(
        r'\{\{%\s*warning\s*%\}\}(.*?)\{\{%\s*/?\s*warning\s*%\}\}',
        r'> **Warning:** \1',
        content, flags=re.DOTALL
    )

    # Note - both styles
    content = re.sub(
        r'\{\{<\s*note\s*>\}\}(.*?)\{\{<?\s*/?\s*note\s*>?\}\}',
        r'> **Note:** \1',
        content, flags=re.DOTALL
    )
    content = re.sub(
        r'\{\{%\s*note\s*%\}\}(.*?)\{\{%\s*/?\s*note\s*%\}\}',
        r'> **Note:** \1',
        content, flags=re.DOTALL
    )

    # Tip - both styles
    content = re.sub(
        r'\{\{<\s*tip\s*>\}\}(.*?)\{\{<?\s*/?\s*tip\s*>?\}\}',
        r'> **Tip:** \1',
        content, flags=re.DOTALL
    )
    content = re.sub(
        r'\{\{%\s*tip\s*%\}\}(.*?)\{\{%\s*/?\s*tip\s*%\}\}',
        r'> **Tip:** \1',
        content, flags=re.DOTALL
    )

    # Important - both styles
    content = re.sub(
        r'\{\{<\s*important\s*>\}\}(.*?)\{\{<?\s*/?\s*important\s*>?\}\}',
        r'> **Important:** \1',
        content, flags=re.DOTALL
    )
    content = re.sub(
        r'\{\{%\s*important\s*%\}\}(.*?)\{\{%\s*/?\s*important\s*%\}\}',
        r'> **Important:** \1',
        content, flags=re.DOTALL
    )

    # Private preview
    content = re.sub(
        r'\{\{<\s*private-preview\s*/?\s*>\}\}',
        '> **Private Preview:** This feature is in private preview.',
        content
    )

    # Public preview
    content = re.sub(
        r'\{\{<\s*public-preview\s*/?\s*>\}\}',
        '> **Public Preview:** This feature is in public preview.',
        content
    )

    return content


def resolve_diagrams(content: str) -> str:
    """Convert {{< diagram "..." >}} to markdown links."""
    return re.sub(
        r'\{\{<\s*diagram\s+"([^"]+)"\s*>\}\}',
        r'[See diagram: \1]',
        content
    )


def resolve_linkbox(content: str) -> str:
    """Convert linkbox shortcodes to plain markdown lists."""
    # Remove multilinkbox wrappers (all variations)
    content = re.sub(r'\{\{<\s*/?\s*multilinkbox\s*>\}\}', '', content)
    content = re.sub(r'\{\{<?\s*/?\s*multilinkbox\s*>?\}\}', '', content)

    # Convert linkbox to H4 + list
    def process_linkbox(match: re.Match) -> str:
        title = match.group(1)
        inner_content = match.group(2).strip()
        return f"**{title}**\n{inner_content}"

    content = re.sub(
        r'\{\{<\s*linkbox\s+title="([^"]+)"\s*>\}\}(.*?)\{\{<?\s*/?\s*linkbox\s*>?\}\}',
        process_linkbox,
        content, flags=re.DOTALL
    )

    # Clean up any remaining linkbox tags
    content = re.sub(r'\{\{<?\s*/?\s*linkbox\s*>?\}\}', '', content)

    return content


def resolve_annotation(content: str) -> str:
    """Convert {{< annotation type="..." >}} to blockquotes."""
    def process_annotation(match: re.Match) -> str:
        ann_type = match.group(1)
        inner_content = match.group(2).strip()
        return f"> **{ann_type}:** {inner_content}"

    content = re.sub(
        r'\{\{<\s*annotation\s+type="([^"]+)"\s*>\}\}(.*?)\{\{<?\s*/?\s*annotation\s*>?\}\}',
        process_annotation,
        content, flags=re.DOTALL
    )

    return content


def resolve_yaml_table(content: str) -> str:
    """Replace yaml-table shortcodes with placeholder."""
    # These are dynamic and can't be fully resolved without the YAML data
    content = re.sub(
        r'\{\{<\s*yaml-table\s+data="([^"]+)"[^>]*>\}\}',
        r'<!-- Dynamic table: \1 - see original docs -->',
        content
    )
    return content


def resolve_yaml_list(content: str) -> str:
    """Replace yaml-list shortcodes with placeholder."""
    content = re.sub(
        r'\{\{<\s*yaml-list\s+data="([^"]+)"[^>]*>\}\}',
        r'<!-- Dynamic list: \1 - see original docs -->',
        content
    )
    return content


def resolve_include_syntax(content: str) -> str:
    """Replace include-syntax shortcodes with placeholder."""
    content = re.sub(
        r'\{\{%\s*include-syntax\s+file="([^"]+)"\s+example="([^"]+)"\s*%\}\}',
        r'<!-- Syntax example: \1 / \2 -->',
        content
    )
    return content


def resolve_other_shortcodes(content: str) -> str:
    """Handle miscellaneous shortcodes."""
    # json-parser widget
    content = re.sub(
        r'\{\{<\s*json-parser\s*>\}\}',
        '<!-- JSON Parser Widget - see original docs -->',
        content
    )

    # sql-commands-table-by-label
    content = re.sub(
        r'\{\{<\s*sql-commands-table-by-label[^>]*>\}\}',
        '<!-- SQL Commands Table - see sql/ directory for individual commands -->',
        content
    )

    # guided-tour-blurb
    content = re.sub(
        r'\{\{<\s*guided-tour-blurb[^>]*>\}\}',
        '',
        content
    )

    # self-managed/terraform-disclaimer
    content = re.sub(
        r'\{\{<\s*self-managed/terraform-disclaimer\s*>\}\}',
        '> **Note:** Terraform configurations may vary based on your environment.',
        content
    )

    # include-example
    content = re.sub(
        r'\{\{<\s*include-example\s+file="([^"]+)"\s+example="([^"]+)"[^>]*>\}\}',
        r'<!-- Example: \1 / \2 -->',
        content
    )

    # Remove multilinkbox closing tags (all variations)
    content = re.sub(r'\{\{<?\s*/?\s*multilinkbox\s*>?\}\}', '', content)

    # Remove tab closing tags
    content = re.sub(r'\{\{<?\s*/?\s*tab\s*>?\}\}', '', content)
    content = re.sub(r'\{\{<?\s*/\s*tabs\s*>?\}\}', '', content)

    # Remove annotation closing tags
    content = re.sub(r'\{\{<?\s*/?\s*annotation\s*>?\}\}', '', content)

    # Remove note/warning/tip closing tags that weren't matched
    content = re.sub(r'\{\{<?\s*/?\s*note\s*>?\}\}', '', content)
    content = re.sub(r'\{\{<?\s*/?\s*warning\s*>?\}\}', '', content)
    content = re.sub(r'\{\{<?\s*/?\s*tip\s*>?\}\}', '', content)
    content = re.sub(r'\{\{<?\s*/?\s*important\s*>?\}\}', '', content)

    # Handle yaml-table with complex attributes
    content = re.sub(
        r'\{\{<\s*yaml-table[^>]*>\}\}',
        '<!-- Dynamic table - see original docs -->',
        content
    )

    # Handle yaml-list with complex attributes
    content = re.sub(
        r'\{\{<\s*yaml-list[^>]*>\}\}',
        '<!-- Dynamic list - see original docs -->',
        content
    )

    # Handle remaining include-md variations
    content = re.sub(
        r'\{\{<\s*include-md[^>]*>\}\}',
        '<!-- Included content - see original docs -->',
        content
    )

    # Handle remaining include shortcodes
    content = re.sub(
        r'\{\{<?\s*include[^>]*>?\}\}',
        '<!-- Included content - see original docs -->',
        content
    )

    # Handle tab opening tags that weren't fully processed
    content = re.sub(
        r'\{\{<\s*tab\s+"([^"]+)"\s*>\}\}',
        r'#### \1\n',
        content
    )
    content = re.sub(
        r'\{\{<\s*tab\s+([^>]+)>\}\}',
        r'#### \1\n',
        content
    )

    # Handle tabs opening tag
    content = re.sub(r'\{\{<\s*tabs\s*>\}\}', '', content)

    # Handle {{% %}} style shortcodes - convert known patterns
    # These are Hugo shortcodes that reference templates

    # views-indexes patterns
    content = re.sub(
        r'\{\{%\s*views-indexes/[^%]+\s*%\}\}',
        '<!-- See views/indexes documentation for details -->',
        content
    )

    # configuration-parameters
    content = re.sub(
        r'\{\{%\s*configuration-parameters\s*%\}\}',
        '<!-- See configuration parameters documentation -->',
        content
    )

    # index_usage patterns
    content = re.sub(
        r'\{\{%\s*index_usage/[^%]+\s*%\}\}',
        '<!-- See index usage documentation -->',
        content
    )

    # best-practices patterns
    content = re.sub(
        r'\{\{%\s*best-practices/[^%]+\s*%\}\}',
        '<!-- See best practices documentation -->',
        content
    )

    # txns patterns
    content = re.sub(
        r'\{\{%\s*txns/[^%]+\s*%\}\}',
        '<!-- See transactions documentation -->',
        content
    )

    # self-managed patterns
    content = re.sub(
        r'\{\{%\s*self-managed/[^%]+\s*%\}\}',
        '<!-- See self-managed installation documentation -->',
        content
    )

    # ingest-data patterns
    content = re.sub(
        r'\{\{%\s*ingest-data/[^%]+\s*%\}\}',
        '<!-- See ingest-data documentation -->',
        content
    )

    # plugins patterns
    content = re.sub(
        r'\{\{%\s*plugins/[^%]+\s*%\}\}',
        '<!-- See plugins documentation -->',
        content
    )

    # yaml-table with {{% %}} style
    content = re.sub(
        r'\{\{%\s*yaml-table[^%]*%\}\}',
        '<!-- Dynamic table - see original docs -->',
        content
    )

    # include-md with {{% %}} style
    content = re.sub(
        r'\{\{%\s*include-md[^%]*%\}\}',
        '<!-- Included content - see original docs -->',
        content
    )

    # include-from-yaml patterns
    content = re.sub(
        r'\{\{%\s*include-from-yaml[^%]*%\}\}',
        '<!-- Dynamic content - see original docs -->',
        content
    )

    # replica-options
    content = re.sub(
        r'\{\{%\s*replica-options\s*%\}\}',
        '<!-- See replica options documentation -->',
        content
    )

    # Remove any remaining Hugo shortcode patterns (cleanup)
    # This catches things like {{% ... %}} that weren't specifically handled
    content = re.sub(
        r'\{\{%\s*/[^%]+\s*%\}\}',  # Closing tags
        '',
        content
    )
    content = re.sub(
        r'\{\{%\s*[^%]+\s*%\}\}',
        lambda m: f'<!-- See original docs: {m.group(0)[3:-3].strip()[:40]} -->' if len(m.group(0)) > 40 else f'<!-- See original docs: {m.group(0)[3:-3].strip()} -->',
        content
    )

    # Final cleanup: remove any remaining {{< ... >}} shortcodes
    content = re.sub(
        r'\{\{<[^>]*>\}\}',
        '',
        content
    )

    # Clean up double/triple newlines
    content = re.sub(r'\n{4,}', '\n\n\n', content)

    return content


def resolve_shortcodes(content: str, base_path: Path) -> str:
    """Resolve all Hugo shortcodes in content."""
    # Order matters - resolve includes first since they may contain other shortcodes

    # Resolve include-md
    content = re.sub(
        r'\{\{<\s*include-md\s+file="([^"]+)"\s*>\}\}',
        lambda m: resolve_include_md(m, base_path),
        content
    )

    # Resolve tabs (before admonitions since tabs may contain them)
    content = resolve_tabs(content)

    # Resolve linkbox
    content = resolve_linkbox(content)

    # Resolve annotation
    content = resolve_annotation(content)

    # Resolve admonitions
    content = resolve_admonitions(content)

    # Resolve diagrams
    content = resolve_diagrams(content)

    # Resolve yaml-table and yaml-list (placeholders)
    content = resolve_yaml_table(content)
    content = resolve_yaml_list(content)

    # Resolve include-syntax (placeholders)
    content = resolve_include_syntax(content)

    # Handle other miscellaneous shortcodes
    content = resolve_other_shortcodes(content)

    return content


def convert_tables_to_lists(content: str) -> Tuple[str, int]:
    """Convert markdown tables with >5 rows to bulleted lists."""
    table_pattern = re.compile(
        r'(\|[^\n]+\|\n)(\|[-:\s|]+\|\n)((?:\|[^\n]+\|\n)+)',
        re.MULTILINE
    )

    count = 0

    def process_table(match: re.Match) -> str:
        nonlocal count
        header_row = match.group(1)
        separator = match.group(2)
        body = match.group(3)

        # Parse header
        headers = [h.strip() for h in header_row.strip().strip('|').split('|')]

        # Parse body rows
        rows = []
        for line in body.strip().split('\n'):
            cells = [c.strip() for c in line.strip().strip('|').split('|')]
            rows.append(cells)

        # Only convert if >5 rows
        if len(rows) <= 5:
            return match.group(0)

        count += 1

        # Convert to bulleted list
        result = []
        for row in rows:
            if len(row) >= 2:
                # Format: **first_col** (type): description
                first = row[0]
                rest = " | ".join(row[1:])
                result.append(f"- **{first}**: {rest}")
            else:
                result.append(f"- {' | '.join(row)}")

        return "\n".join(result) + "\n"

    content = table_pattern.sub(process_table, content)
    return content, count


def extract_title(content: str) -> str:
    """Extract the title from markdown content."""
    match = re.search(r'^#\s+(.+)$', content, re.MULTILINE)
    if match:
        return match.group(1).strip()
    return "Untitled"


def infer_doc_type(content: str, path: Path) -> str:
    """Infer document type from content and path."""
    path_str = str(path).lower()
    content_lower = content.lower()

    if 'howto' in path_str or 'guide' in path_str:
        return 'howto'
    if 'tutorial' in path_str:
        return 'tutorial'
    if 'troubleshoot' in path_str or 'faq' in path_str:
        return 'troubleshooting'
    if 'concept' in path_str or path_str.startswith('.claude/skills/materialize-docs/concepts'):
        return 'concept'
    if '/sql/' in path_str:
        return 'reference'
    if 'overview' in content_lower[:500]:
        return 'overview'
    return 'reference'


def infer_product_area(path: Path) -> str:
    """Infer product area from path."""
    path_str = str(path)

    if '/sql/' in path_str:
        # Further categorize SQL docs
        if 'source' in path_str.lower():
            return 'Sources'
        if 'sink' in path_str.lower():
            return 'Sinks'
        if 'view' in path_str.lower():
            return 'Views'
        if 'index' in path_str.lower():
            return 'Indexes'
        if 'cluster' in path_str.lower():
            return 'Clusters'
        if 'connection' in path_str.lower():
            return 'Connections'
        return 'SQL'
    if '/concepts/' in path_str:
        return 'Concepts'
    if '/ingest-data/' in path_str:
        return 'Sources'
    if '/serve-results/' in path_str:
        return 'Sinks'
    if '/security/' in path_str:
        return 'Security'
    if '/install' in path_str:
        return 'Deployment'
    if '/manage/' in path_str:
        return 'Operations'
    if '/transform-data/' in path_str:
        return 'SQL'

    return 'General'


def infer_status(content: str) -> str:
    """Infer stability status from content."""
    content_lower = content.lower()
    if 'private preview' in content_lower:
        return 'experimental'
    if 'public preview' in content_lower or 'beta' in content_lower:
        return 'beta'
    if 'deprecated' in content_lower:
        return 'deprecated'
    return 'stable'


def infer_complexity(content: str, path: Path) -> str:
    """Infer complexity level from content and path."""
    content_lower = content.lower()

    # Advanced indicators
    advanced_terms = ['dataflow', 'arrangement', 'introspection', 'optimization',
                      'performance tuning', 'advanced', 'internal']
    if any(term in content_lower for term in advanced_terms):
        return 'advanced'

    # Beginner indicators
    beginner_terms = ['getting started', 'quickstart', 'introduction', 'overview',
                      'what is', 'basic']
    if any(term in content_lower for term in beginner_terms):
        return 'beginner'

    return 'intermediate'


def extract_keywords(content: str, title: str) -> List[str]:
    """Extract keywords from content."""
    keywords = [title]

    # Extract SQL command names
    sql_commands = re.findall(r'\b(CREATE|ALTER|DROP|SELECT|INSERT|UPDATE|DELETE|SHOW|EXPLAIN)\s+(\w+)', content.upper())
    for cmd in sql_commands[:5]:
        keywords.append(' '.join(cmd))

    # Extract important terms
    important_terms = re.findall(r'\*\*([^*]+)\*\*', content)
    keywords.extend(important_terms[:5])

    return list(set(keywords))[:10]


def generate_metadata(content: str, path: Path) -> Dict:
    """Generate YAML front matter metadata."""
    title = extract_title(content)

    # Extract first paragraph as description
    paragraphs = re.findall(r'^[^#\n][^\n]+', content, re.MULTILINE)
    description = paragraphs[0].strip() if paragraphs else f"Documentation for {title}"
    description = description[:200] + "..." if len(description) > 200 else description

    # Calculate relative path for canonical URL
    rel_path = str(path).replace('.claude/skills/materialize-docs/', '')
    rel_path = rel_path.replace('/index.md', '/').replace('.md', '/')

    metadata = {
        'title': title,
        'description': description,
        'doc_type': infer_doc_type(content, path),
        'product_area': infer_product_area(path),
        'audience': 'developer',
        'status': infer_status(content),
        'complexity': infer_complexity(content, path),
        'keywords': extract_keywords(content, title),
        'canonical_url': f"https://materialize.com/docs/{rel_path}",
    }

    return metadata


def add_preamble(content: str, doc_type: str) -> str:
    """Add progressive-disclosure preamble after title."""
    # Find title
    title_match = re.search(r'^(#\s+.+\n)', content, re.MULTILINE)
    if not title_match:
        return content

    title = title_match.group(1)
    rest = content[title_match.end():]

    # Check if preamble already exists
    if '## Purpose' in rest[:500]:
        return content

    # Extract first paragraph for Purpose
    first_para_match = re.search(r'^([^#\n][^\n]+(?:\n[^#\n][^\n]+)*)', rest.strip())
    purpose = first_para_match.group(1).strip() if first_para_match else ""

    # Generate session starter based on doc_type
    if doc_type == 'reference':
        session_starter = "If you need to understand the syntax and options for this command, you're in the right place."
    elif doc_type == 'howto':
        session_starter = "Follow the steps below to complete this task."
    elif doc_type == 'concept':
        session_starter = "Read this to understand how this concept works in Materialize."
    else:
        session_starter = "This page provides detailed documentation for this topic."

    # Build preamble
    preamble = f"""
## Purpose
{purpose}

{session_starter}

"""

    return title + preamble + rest


def add_section_summaries(content: str) -> str:
    """Add brief summaries after H2 headings that lack them."""
    lines = content.split('\n')
    result = []
    i = 0

    while i < len(lines):
        line = lines[i]
        result.append(line)

        # Check if this is an H2 heading
        if line.startswith('## ') and not line.startswith('## Purpose') and not line.startswith('## When'):
            # Check if next non-empty line is content or another heading/code
            j = i + 1
            while j < len(lines) and not lines[j].strip():
                result.append(lines[j])
                j += 1

            if j < len(lines):
                next_line = lines[j].strip()
                # If next content is a code block, table, or heading, add a summary placeholder
                if next_line.startswith('```') or next_line.startswith('|') or next_line.startswith('#'):
                    heading_text = line[3:].strip()
                    result.append(f"This section covers {heading_text.lower()}.")
                    result.append("")

            i = j - 1  # Will be incremented at end of loop

        i += 1

    return '\n'.join(result)


def normalize_code_blocks(content: str) -> str:
    """Ensure all code blocks have language specifiers."""
    # Find code blocks without language
    def add_language(match: re.Match) -> str:
        code = match.group(1)
        # Try to infer language
        if code.strip().startswith('SELECT') or code.strip().startswith('CREATE') or code.strip().startswith('ALTER'):
            return f'```sql\n{code}```'
        if code.strip().startswith('{') or code.strip().startswith('['):
            return f'```json\n{code}```'
        if code.strip().startswith('$') or code.strip().startswith('#'):
            return f'```bash\n{code}```'
        return f'```text\n{code}```'

    content = re.sub(r'```\n((?:(?!```)[\s\S])*?)```', add_language, content)
    return content


def rewrite_links(content: str) -> str:
    """Rewrite links to use relative paths within the skill."""
    # Convert absolute materialize.com links to relative
    content = re.sub(
        r'\]\(https?://(?:www\.)?materialize\.com/docs/([^)]+)\)',
        r'](../\1)',
        content
    )

    # Clean up double slashes
    content = re.sub(r'\]\(\.\.//', r'](../', content)

    return content


def process_file(file_path: Path, stats: ProcessingStats, force_reprocess: bool = False) -> bool:
    """Process a single markdown file."""
    try:
        content = file_path.read_text()
        original_content = content

        # Skip if already has front matter with our metadata (unless forcing reprocess)
        has_front_matter = content.startswith('---\n') and 'doc_type:' in content[:500]
        has_shortcodes = '{{<' in content or '{{%' in content

        if has_front_matter and not has_shortcodes and not force_reprocess:
            stats.files_skipped += 1
            return True

        # If file has front matter, extract content after it for reprocessing
        if has_front_matter:
            # Find end of front matter
            end_marker = content.find('---\n', 4)
            if end_marker > 0:
                front_matter_yaml = content[4:end_marker]
                content = content[end_marker + 4:].strip()
                # We'll regenerate metadata but preserve the content

        # 1. Resolve shortcodes
        content = resolve_shortcodes(content, file_path.parent)
        shortcode_count = len(re.findall(r'\{\{[<%]', original_content)) - len(re.findall(r'\{\{[<%]', content))
        stats.shortcodes_resolved += shortcode_count

        # 2. Convert tables to lists
        content, tables_converted = convert_tables_to_lists(content)
        stats.tables_converted += tables_converted

        # 3. Generate and add metadata
        metadata = generate_metadata(content, file_path)

        # 4. Add preamble
        content = add_preamble(content, metadata['doc_type'])

        # 5. Add section summaries
        content = add_section_summaries(content)

        # 6. Normalize code blocks
        content = normalize_code_blocks(content)

        # 7. Rewrite links
        content = rewrite_links(content)

        # Build final content with front matter
        front_matter = yaml.dump(metadata, default_flow_style=False, allow_unicode=True)
        final_content = f"---\n{front_matter}---\n\n{content}"

        # Write back
        file_path.write_text(final_content)
        stats.files_processed += 1
        stats.metadata_added += 1

        return True

    except Exception as e:
        stats.errors.append(f"{file_path}: {str(e)}")
        return False


def create_glossary(skill_dir: Path) -> None:
    """Create _GLOSSARY.md file."""
    glossary_content = """---
title: Glossary
description: Definitions of Materialize-specific terms
doc_type: reference
product_area: General
---

# Glossary

This glossary defines key Materialize-specific terms.

## A

**Arrangement**: An indexed, incrementally maintained collection stored in memory. Arrangements enable fast point lookups and are the foundation of Materialize's incremental view maintenance. See [Indexes](concepts/indexes/index.md).

## C

**Cluster**: A pool of isolated compute resources that runs dataflows. Each cluster has one or more replicas. See [Clusters](concepts/clusters/index.md).

**Connection**: A reusable object that stores credentials and configuration for connecting to external systems. See [CREATE CONNECTION](sql/create-connection/index.md).

## D

**Dataflow**: A computation graph that processes streaming data incrementally. Dataflows are created automatically when you create sources, materialized views, or indexes.

## I

**Index**: An arrangement on a view that enables fast point lookups. See [CREATE INDEX](sql/create-index/index.md).

## M

**Materialized View**: A view whose results are persisted and incrementally updated as source data changes. See [CREATE MATERIALIZED VIEW](sql/create-materialized-view/index.md).

## R

**Replica**: A copy of a cluster's compute resources. Multiple replicas provide fault tolerance.

## S

**Schema**: A namespace within a database that contains tables, views, and other objects. See [Namespaces](concepts/namespaces/index.md).

**Sink**: A connection that exports data from Materialize to an external system. See [CREATE SINK](sql/create-sink/index.md).

**Source**: A connection to an external system that streams data into Materialize. See [CREATE SOURCE](sql/create-source/index.md).

## V

**View**: A named query that is executed when referenced. Unlike materialized views, regular views do not persist results. See [CREATE VIEW](sql/create-view/index.md).
"""

    glossary_path = skill_dir / "_GLOSSARY.md"
    glossary_path.write_text(glossary_content)
    print(f"Created {glossary_path}")


def create_vocabulary(skill_dir: Path) -> None:
    """Create _VOCABULARY.yaml file."""
    vocabulary = {
        'terms': {
            'source': {
                'canonical': 'source',
                'aliases': ['data source', 'ingestion source'],
                'definition': 'Connection to external system streaming data into Materialize',
                'not_to_be_confused_with': ['table', 'view'],
            },
            'materialized_view': {
                'canonical': 'materialized view',
                'aliases': ['matview', 'MV', 'mat view'],
                'definition': 'A view whose results are persisted and incrementally updated',
                'not_to_be_confused_with': ['view', 'index'],
            },
            'cluster': {
                'canonical': 'cluster',
                'aliases': [],
                'definition': 'A pool of compute resources that runs dataflows',
                'not_to_be_confused_with': ['replica', 'database cluster'],
            },
            'arrangement': {
                'canonical': 'arrangement',
                'aliases': [],
                'definition': 'An indexed, incrementally maintained collection in memory',
            },
            'dataflow': {
                'canonical': 'dataflow',
                'aliases': ['data flow'],
                'definition': 'A computation graph that processes streaming data',
            },
            'index': {
                'canonical': 'index',
                'aliases': [],
                'definition': 'An arrangement on a view enabling fast point lookups',
                'not_to_be_confused_with': ['arrangement', 'primary key'],
            },
            'sink': {
                'canonical': 'sink',
                'aliases': ['data sink', 'output'],
                'definition': 'Connection that exports data from Materialize to external systems',
                'not_to_be_confused_with': ['source', 'table'],
            },
            'replica': {
                'canonical': 'replica',
                'aliases': ['cluster replica'],
                'definition': 'A copy of cluster compute resources for fault tolerance',
                'not_to_be_confused_with': ['cluster'],
            },
            'connection': {
                'canonical': 'connection',
                'aliases': [],
                'definition': 'Reusable object storing credentials for external system access',
            },
            'secret': {
                'canonical': 'secret',
                'aliases': [],
                'definition': 'Securely stored credential value used by connections',
            },
        }
    }

    vocab_path = skill_dir / "_VOCABULARY.yaml"
    with open(vocab_path, 'w') as f:
        yaml.dump(vocabulary, f, default_flow_style=False, allow_unicode=True)
    print(f"Created {vocab_path}")


def create_sql_commands_reference(skill_dir: Path) -> None:
    """Create _SQL_COMMANDS.md quick reference."""
    sql_commands_content = """---
title: SQL Command Reference
description: Quick reference of all Materialize SQL commands
doc_type: reference
product_area: SQL
---

# SQL Command Reference

Quick reference of all SQL commands with one-line descriptions.

## Data Definition (DDL)

### Sources
- **`CREATE SOURCE`**: Define an external data source for ingestion — [docs](sql/create-source/index.md)
- **`ALTER SOURCE`**: Modify source configuration — [docs](sql/alter-source/index.md)
- **`DROP SOURCE`**: Remove a source — [docs](sql/drop-source/index.md)
- **`SHOW SOURCES`**: List sources — [docs](sql/show-sources/index.md)

### Views
- **`CREATE VIEW`**: Create a named query — [docs](sql/create-view/index.md)
- **`CREATE MATERIALIZED VIEW`**: Create an incrementally maintained view — [docs](sql/create-materialized-view/index.md)
- **`ALTER VIEW`**: Modify view properties — [docs](sql/alter-view/index.md)
- **`ALTER MATERIALIZED VIEW`**: Modify materialized view properties — [docs](sql/alter-materialized-view/index.md)
- **`DROP VIEW`**: Remove a view — [docs](sql/drop-view/index.md)
- **`DROP MATERIALIZED VIEW`**: Remove a materialized view — [docs](sql/drop-materialized-view/index.md)

### Indexes
- **`CREATE INDEX`**: Create an index for fast lookups — [docs](sql/create-index/index.md)
- **`ALTER INDEX`**: Modify index properties — [docs](sql/alter-index/index.md)
- **`DROP INDEX`**: Remove an index — [docs](sql/drop-index/index.md)

### Sinks
- **`CREATE SINK`**: Define an output destination — [docs](sql/create-sink/index.md)
- **`ALTER SINK`**: Modify sink configuration — [docs](sql/alter-sink/index.md)
- **`DROP SINK`**: Remove a sink — [docs](sql/drop-sink/index.md)

### Tables
- **`CREATE TABLE`**: Create a table for manual data entry — [docs](sql/create-table/index.md)
- **`ALTER TABLE`**: Modify table properties — [docs](sql/alter-table/index.md)
- **`DROP TABLE`**: Remove a table — [docs](sql/drop-table/index.md)

### Clusters
- **`CREATE CLUSTER`**: Create compute resources — [docs](sql/create-cluster/index.md)
- **`ALTER CLUSTER`**: Modify cluster configuration — [docs](sql/alter-cluster/index.md)
- **`DROP CLUSTER`**: Remove a cluster — [docs](sql/drop-cluster/index.md)

### Connections
- **`CREATE CONNECTION`**: Store external system credentials — [docs](sql/create-connection/index.md)
- **`ALTER CONNECTION`**: Modify connection settings — [docs](sql/alter-connection/index.md)
- **`DROP CONNECTION`**: Remove a connection — [docs](sql/drop-connection/index.md)

### Schemas & Databases
- **`CREATE DATABASE`**: Create a database — [docs](sql/create-database/index.md)
- **`CREATE SCHEMA`**: Create a schema — [docs](sql/create-schema/index.md)
- **`DROP DATABASE`**: Remove a database — [docs](sql/drop-database/index.md)
- **`DROP SCHEMA`**: Remove a schema — [docs](sql/drop-schema/index.md)

## Data Manipulation (DML)

- **`SELECT`**: Query data — [docs](sql/select/index.md)
- **`INSERT`**: Add rows to a table — [docs](sql/insert/index.md)
- **`UPDATE`**: Modify existing rows — [docs](sql/update/index.md)
- **`DELETE`**: Remove rows — [docs](sql/delete/index.md)
- **`COPY FROM`**: Bulk load data — [docs](sql/copy-from/index.md)
- **`COPY TO`**: Bulk export data — [docs](sql/copy-to/index.md)
- **`SUBSCRIBE`**: Stream changes from a relation — [docs](sql/subscribe/index.md)

## Query Introspection

- **`EXPLAIN PLAN`**: Show query execution plan — [docs](sql/explain-plan/index.md)
- **`EXPLAIN TIMESTAMP`**: Show timestamp selection — [docs](sql/explain-timestamp/index.md)
- **`EXPLAIN ANALYZE`**: Execute and profile a query — [docs](sql/explain-analyze/index.md)

## Access Control (RBAC)

- **`CREATE ROLE`**: Create a database role — [docs](sql/create-role/index.md)
- **`ALTER ROLE`**: Modify role properties — [docs](sql/alter-role/index.md)
- **`DROP ROLE`**: Remove a role — [docs](sql/drop-role/index.md)
- **`GRANT`**: Grant privileges — [docs](sql/grant-privilege/index.md)
- **`REVOKE`**: Remove privileges — [docs](sql/revoke-privilege/index.md)

## Session Management

- **`SET`**: Set session variables — [docs](sql/set/index.md)
- **`SHOW`**: Display session or object information — [docs](sql/show/index.md)
- **`BEGIN`**: Start a transaction — [docs](sql/begin/index.md)
- **`COMMIT`**: Commit a transaction — [docs](sql/commit/index.md)
"""

    sql_ref_path = skill_dir / "_SQL_COMMANDS.md"
    sql_ref_path.write_text(sql_commands_content)
    print(f"Created {sql_ref_path}")


def main():
    """Main processing function."""
    stats = ProcessingStats()

    print("=" * 60)
    print("Materialize Docs Skill Upgrade")
    print("=" * 60)

    # Find all markdown files
    md_files = list(SKILL_DIR.glob("**/*.md"))
    print(f"Found {len(md_files)} markdown files to process")

    # Process each file
    for i, file_path in enumerate(md_files):
        # Skip SKILL.md and routing artifacts
        if file_path.name in ['SKILL.md', '_GLOSSARY.md', '_VOCABULARY.yaml', '_SQL_COMMANDS.md']:
            continue

        if (i + 1) % 50 == 0:
            print(f"Processing {i + 1}/{len(md_files)}...")

        process_file(file_path, stats)

    # Create routing artifacts
    print("\nCreating routing artifacts...")
    create_glossary(SKILL_DIR)
    create_vocabulary(SKILL_DIR)
    create_sql_commands_reference(SKILL_DIR)

    # Print summary
    print("\n" + "=" * 60)
    print("Processing Summary")
    print("=" * 60)
    print(f"Files processed: {stats.files_processed}")
    print(f"Files skipped: {stats.files_skipped}")
    print(f"Shortcodes resolved: {stats.shortcodes_resolved}")
    print(f"Tables converted: {stats.tables_converted}")
    print(f"Metadata added: {stats.metadata_added}")

    if stats.errors:
        print(f"\nErrors ({len(stats.errors)}):")
        for error in stats.errors[:10]:
            print(f"  - {error}")
        if len(stats.errors) > 10:
            print(f"  ... and {len(stats.errors) - 10} more")

    print("\nDone!")


if __name__ == "__main__":
    main()
