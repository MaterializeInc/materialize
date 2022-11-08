This doc walks you through the process of contributing to Materialize's reference documentation.

# Stylistic conventions

1.  Strive for minimal, to the point documentation.
    Use the design documentation process to discuss the broader perspective, background, rationale, and alternatives aboud decisions that evolve the codebase.
1.  Don't duplicate information.
    For example, if you are writing reference documentation of some protocol or library in the crate, put a link to the publicly hosted API docs.
1.  When referencing code, prefer relative links.
    This will ensure that the documentation always refers the current branch.

# Asset management workflow

1.  Add assets (such as `*.png` figures) to [the `assets` subfolder](./assets/).
1.  Add the sources for each asset [in our Excalidraw project](https://app.excalidraw.com/o/6NqJ5ikTEpv/4jWfVZot2bS).
    1.  If you are a member of [the MaterializeInc org](https://github.com/orgs/MaterializeInc/people):
        1. [Join our Excalidraw workspace](app.excalidraw.com/redeem/6NqJ5ikTEpv/5Be393wLTO2) useing your `@materialize.com` email address.
        1. Create a separate scene for evey new image. Align the name of the image and the name of the scene for better discoverability.
    1.  If you are an external contributor:
        1. Ping a member of [the MaterializeInc org](https://github.com/orgs/MaterializeInc/people) in GitHub or [in the community Slack](https://materialize.com/s/chat) to get the sources you need.
        1. Attach the `*.excalidraw` sources to your PR, so the person that approves and merges the PR can make them available for others [in our Excalidraw project](https://app.excalidraw.com/o/6NqJ5ikTEpv/4jWfVZot2bS) before merging.
