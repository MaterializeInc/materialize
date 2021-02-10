Additional support for hacking on Materialize in Emacs
======================================================

This directory contains a major mode that allows syntax-highlighting testdrive (`.td`)
files.

## Installation

This major mode depends on [`polymode`](https://github.com/polymode/polymode).

There are several possible ways to install, but the only one that I know works for sure
is to use the builtin package.el:

Call `package-install-file` and point it at the mz-testdrive.el file:

> <kbd>M-x</kbd> `package-install-file` <kbd>RET</kbd> `mz-testdrive.el`

Then add `(mz-testdrive-enable)` to your `init.el`.

Testdrive files will be registered as fundamentally `shell-script-mode` files, which
means any hooks you have for `shell-script-mode` will run, but they may not make sense.
For example, `shellcheck` doesn't make sense in testdrive files. I recommend adding
something like the following to your `init.el` just after `(mz-testdrive-enable)`:

```elisp
(add-hook 'mz-testdrive-mode-hook
    (lambda ()
        (flymake-mode 0))) ; same thing would work for flycheck-mode if you use that
```
