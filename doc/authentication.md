# GitHub Authentication

Reconciling security and convenience is genuinely difficult. Fortunately, GitHub
gives us plenty of options. Most importantly, if you are a member of the
MaterializeInc organization, you already have [two-factor authentication on
GitHub] enabled. Otherwise, you couldn't have joined the GitHub organization.

## The Limits of SSH

Our [default setup](https://github.com/MaterializeInc/mtrlz-setup) relies on SSH
and a newly created key registered with GitHub. This works for most settings,
but not when repositories are cloned via the `https` URLs featured so
prominently in GitHub's web interface and also used by [GitHub
Desktop](https://desktop.github.com). Since we have two-factor authentication
enabled, we cannot just enter user name and password on the command line either.

## The Joys of the Personal Access Token

On macOS, you can use a [personal access token] to make the user experience
comparably seamless:

 1. [Create a personal access token] for GitHub. The new access token should
    have the **repo** scope, no more. Once generated, leave the window or tab
    showing the new token alone. You'll need it in a little while.

 2. [Enable the macOS keychain] for credential storage via the
    `git-credential-osxkeychain` helper shipped with modern versions of git
    (suprisingly, even the version Apple ships):
    ```bash
    git config --global credential.helper osxkeychain
    ```

 3. Perform a remote operation:
    ```bash
    git fetch origin
    ```
    When prompted, enter your GitHub user name. When prompted for your password,
    copy the token from the still open window from step 1 and paste it into the
    terminal. If you perform the fetch again, it should just work without
    prompting again.

 4. Close the browser window showing your personal access token. Select some
    random text anywhere and copy it.

Happy, happy, joy, joy!

[personal access token]: https://help.github.com/en/articles/creating-a-personal-access-token-for-the-command-line
[two-factor authentication on GitHub]: https://help.github.com/en/articles/accessing-github-using-two-factor-authentication
[Create a personal access token]: https://help.github.com/en/articles/creating-a-personal-access-token-for-the-command-line
[Enable the macOS keychain]: https://help.github.com/en/articles/caching-your-github-password-in-git
