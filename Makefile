VENV := precommit_venv
HOOKS := .git/hooks/pre-commit

# PRE-COMMIT HOOKS

$(VENV): .requirements-precommit.txt
	rm -rf $(VENV)
	virtualenv -p python3.6 $(VENV)
	$(VENV)/bin/pip install -r .requirements-precommit.txt

.PHONY: env
env: $(VENV)

.PHONY: clean-env
clean-env:
	rm -rf $(VENV)

.PHONY: install-hooks
install-hooks: $(VENV) .pre-commit-config.yaml
	$(VENV)/bin/pre-commit install -f --install-hooks
	cargo +nightly fmt --help > /dev/null || rustup component add rustfmt-preview --toolchain nightly
	cargo +nightly clippy --help > /dev/null || cargo +nightly install clippy


.PHONY: clean-hooks
clean-hooks:
	rm -rf $(HOOKS)


# LINTING

.PHONY: lint
lint:
	 cargo +nightly fmt

.PHONY: clean-lint
clean-lint:
	find . -type f -name *.rs.bk -delete

.PHONY: clippy
clippy:
	cargo +nightly clippy --all-features


# TESTING

.PHONY: test
test:
	cargo test --all-features


# BENCHMARKING

.PHONY: benchmark
benchmark:
	cargo +nightly bench


# DOCS

.PHONY: doc
doc:
	cargo doc --no-deps --all-features

.PHONY: doc-local
doc-local:
	cargo doc --no-deps --all-features --open


# BUILDING

.PHONY: build
build:
	cargo build --all-features

.PHONY: release
release:
	cargo build --all-features --release


# ALL
.PHONY: clean
clean: clean-env clean-hooks clean-lint
	cargo clean
