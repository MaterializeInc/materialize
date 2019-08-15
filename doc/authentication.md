# GitHub Authentication

Reconciling security and convenience is genuinely difficult. Fortunately, GitHub gives us plenty
of options.

## The Limits of SSH

Our [default setup](https://github.com/MaterializeInc/mtrlz-setup) relies on SSH and a newly
created key registered with GitHub. This works for all user accounts, independent of whether
they have two-factor authentication enabled or not.

**If you haven't enabled [two-factor authentication on GitHub](https://help.github.com/en/articles/accessing-github-using-two-factor-authentication#using-two-factor-authentication-with-the-command-line),
go directly to your settings and enable two-factor authentication. Do not read ahead. Do
not collect lunch.**

SSH, however, won't work for repositories cloned via `https` URLs, which feature prominently
in GitHub's web interface and are used by [GitHub Desktop](https://desktop.github.com). For
accounts without two-factor authentication, you still can enter user name and password on the
command line.

## The Joys of the Personal Access Token

For accounts with two-factor authentication accessed from macOS, you can make working with
repositories cloned via `https` as seamless as for those cloned via SSH.

 1. [Create a personal access token](https://help.github.com/en/articles/creating-a-personal-access-token-for-the-command-line)
    for GitHub. The new access token should have the **repo** scope, no more. Once generated,
    leave the window or tab showing the new token alone. You'll need it in a little while.
 2. [Enable the macOS keychain](https://help.github.com/en/articles/caching-your-github-password-in-git)
    for credential storage via the `git-credential-osxkeychain` helper shipped with modern versions
    of git (suprisingly, even the version Apple ships):
    ```bash
    git config --global credential.helper osxkeychain
    ```
 3. Perform a remote operation:
    ```bash
    git fetch origin
    ```
    When prompted, enter your GitHub user name. When prompted for your password, copy the token from
    the still open window from step 1 and paste it into the terminal. If you perform the fetch again,
    it should just work without prompting again.
 4. Close the browser window showing your personal access token. Select some random text anywhere and
    copy it.

Happy, happy, joy, joy!
