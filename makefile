.PHONEY: coverage, test-32-bit, docs, docs-open

coverage-html:
	mkdir -p target/coverage/html
	mkdir -p target/coverage/raw
	CARGO_INCREMENTAL=0 RUSTFLAGS='-Cinstrument-coverage' LLVM_PROFILE_FILE='target/coverage/raw/cargo-test-%p-%m.profraw' cargo test
	grcov . --binary-path ./target/debug/deps/ -s . -t html --branch --ignore-not-existing --ignore '../*' --ignore "/*" -o target/coverage/html
	rm target/coverage/raw/*.profraw
	open target/coverage/html/index.html

coverage-lcov:
	mkdir -p target/coverage/lcov
	mkdir -p target/coverage/raw
	CARGO_INCREMENTAL=0 RUSTFLAGS='-Cinstrument-coverage' LLVM_PROFILE_FILE='target/coverage/raw/cargo-test-%p-%m.profraw' cargo test
	grcov . --binary-path ./target/debug/deps/ -s . -t lcov --branch --ignore-not-existing --ignore '../*' --ignore "/*" -o target/coverage/lcov/tests.lcov
	rm target/coverage/raw/*.profraw

docs:
	cargo +nightly doc

docs-open:
	cargo +nightly doc --open

test-32-bit:
	docker run --rm -v "$(PWD)":/usr/src/myapp -w /usr/src/myapp i386/rust:1.42.0 cargo test
