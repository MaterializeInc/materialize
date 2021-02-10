;;; mz-testdrive.el --- Major Mode for testdrive files
;
;; Copyright Materialize, Inc. All rights reserved.
;;
;; Use of this software is governed by the Business Source License
;; included in the LICENSE file at the root of this repository.
;;
;; As of the Change Date specified in that file, in accordance with
;; the Business Source License, use of this software will be governed
;; by the Apache License, Version 2.0.

;; Author: Brandon W Maister <bwm@materialize.io>
;; Version: 0.1
;; Package-Requires: ((polymode "0.2.2"))
;; Keywords: testing
;; URL: https://github.com/MaterializeInc/materialize/tree/main/misc/editor/emacs

;;; Commentary:

;; This package provides a major mode which makes interacting with testdrive files
;; somewhat more pleasant.

(require 'polymode)

;;;###autoload
(setq poly-mztd-subsec-end
      (rx (or
           (seq line-start (not whitespace))
           "

")))

;;;###autoload
(define-innermode poly-mztd-sql-innermode
  :mode 'sql-mode
  :head-matcher (rx (seq line-start (or ">" "!") " "))
  :tail-matcher poly-mztd-subsec-end
  :head-mode 'host
  :tail-mode 'host
  )

;;;###autoload
(define-innermode poly-mztd-set-json-innermode
  :mode 'json-mode
  :head-matcher (rx "set " (+ word) "=")
  :tail-matcher poly-mztd-subsec-end
  :head-mode 'host
  :tail-mode 'host)

;;;###autoload
(define-auto-innermode poly-mztd-file-contents-innermode
  :head-matcher (rx line-start "$ file-append")
  :tail-matcher "

"
  :mode-matcher (cons (rx "path=" (+ word) "." (group (+ word))) 1)
  :head-mode 'host
  :tail-mode 'host)

;;;###autoload
(define-innermode poly-mztd-line-delimited-json-innermode
  :mode 'json-mode
  :head-matcher (rx line-start "{")
  :tail-matcher (rx "}" line-end)
  :head-mode 'host
  :tail-mode 'host)


;;;###autoload
(define-hostmode poly-mztd-hostmode :mode 'shell-script-mode)

;;;###autoload
(defvar poly-mz-testdrive-mode-hook ()
  "hooks that are run for `mz-testdrive-mode' and all submodes")

;;;###autoload
(define-polymode mz-testdrive-mode
  :hostmode 'poly-mztd-hostmode
  :innermodes '(poly-mztd-sql-innermode
                poly-mztd-set-json-innermode
                poly-mztd-line-delimited-json-innermode
                poly-mztd-file-contents-innermode))

;;;###autoload
(defun mz-testdrive-enable ()
  "Enable mz-testdrive mode for all files that end in \\.td, and configure some sub-modes"
  (add-hook 'mz-testdrive-mode-hook
            (lambda ()
              (cond
               ((eql major-mode 'sql-mode)
                (sql-set-product
                 (if (assoc 'materialize sql-product-alist)
                     "materialize"
                   "postgres"))))))

  (add-to-list 'auto-mode-alist '("\\.td\\'" . mz-testdrive-mode))
  'mz-testdrive-mode)

(provide 'mz-testdrive)

;;; mz-testdrive.el ends here
