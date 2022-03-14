---
title: "Stylesheet"
---

The following page works as a quick visual demo of how we render common HTML elements using our stylesheet.

# h1

## h2

### h3

#### h4

## h2 with `code`

th | th2
---|----
td | td 2

**bold**

_underline_

`code`

[Linked `code`](/)

```shell
code block
```

[link](#h1)

**Unordered List:**
- ul1
- ul2
- ul3

**Ordered List:**
1. ol1
2. ol2
3. ol3

When an ordered list has `<p>` tags (in markdown, multiple linebreaks between items), it gets custom styles:

1. This is an ordered list with paragraph, useful for step-by-step instructions.

2. Testing out how this works.

## Shortcodes

### `diagram` shortcode

Diagrams from [bottlecaps.de/rr/ui](https://www.bottlecaps.de/rr/ui):

{{< diagram "join-type.svg" >}}

### `note` shortcode

{{< note >}}
This is a note.
{{</ note >}}

### `warning` shortcode

{{< warning >}}
This is a warning.
{{</ warning >}}

### `beta` shortcode

{{< beta >}}
This is a beta notice.
{{</ beta >}}

### `cloud_notice` shortcode

{{< cloud-notice >}}


### `cta` shortcode

{{< cta href="/install" >}}
This is a CTA button
{{</ cta >}}

{{< cta href="/install" full_width="true" >}}
This is a "full_width" CTA button
{{</ cta >}}

### `callout` shortcode

Used for prominent messages. Optionally can include a primary and secondary CTA button using `primary_url`, `primary_text`, `secondary_url`, `secondary_text` in shortcode params.

**Primary Only:**
{{< callout primary_url="https://materialize.com/docs/get-started/" primary_text="Get Started" >}}
  # Header

  Some text and the closing button is specified in the shortcode top.
{{</ callout >}}

**Primary and Secondary:**
{{< callout primary_url="/" primary_text="Primary" secondary_url="/" secondary_text="Secondary">}}
  # Header

  This example has two buttons!
{{</ callout >}}

### `linkbox` shortcode

Used to render categorized lists of links, most helpful for "routing" pages like the homepage.

{{< linkbox icon="bulb" title="Linkbox One" >}}
- [Link One](/)
- [Second Link](/)
- [Link number three](/)
{{</ linkbox >}}

### `multilinkbox` shortcode

Group multiple linkboxes together to form a grid:

{{< multilinkbox >}}
{{< linkbox icon="bulb" title="Linkbox One" >}}
- [Link One](/)
- [Second Link](/)
- [Link number three](/)
{{</ linkbox >}}
{{< linkbox icon="book" title="Linkbox Two" >}}
- [Link One](/)
- [Second Link](/)
- [Link number three](/)
{{</ linkbox >}}
{{< linkbox icon="doc" title="Linkbox Three" >}}
- [Link One](/)
- [Second Link](/)
- [Link number three](/)
{{</ linkbox >}}
{{< linkbox icon="touch" title="Linkbox Four" >}}
- [Link One](/)
- [Second Link](/)
- [Link number three](/)
{{</ linkbox >}}
{{</ multilinkbox >}}

### `tabs` shortcode

{{< tabs >}}
{{< tab "Tab 1">}}
block1 Tab1
{{< /tab >}}
{{< tab "Tab 2">}}
block1 Tab2
{{< /tab >}}
{{< /tabs >}}

{{< tabs >}}
{{< tab "Tab 1">}}
block2 Tab1
{{< /tab >}}
{{< tab "Tab 2">}}
Block2 Tab2
{{< /tab >}}
{{< /tabs >}}
