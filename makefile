.PHONEY: coverage, test-32-bit, docs, docs-open

coverage:
	CARGO_INCREMENTAL=0 \
	RUSTFLAGS="-Zprofile -Ccodegen-units=1 -Cinline-threshold=0 -Clink-dead-code -Coverflow-checks=off -Zno-landing-pads" \
	cargo +nightly build && \
	cargo +nightly test && \
	zip -0 ./target/debug/ccov.zip `find ./target \( -name "jammdb*.gc*" \) -print` && \
	grcov ./target/debug/ccov.zip -s . -t lcov --llvm --branch --ignore-not-existing --ignore "/*" -o ./target/debug/lcov.info && \
	genhtml -o ./target/debug/coverage/ --show-details --highlight --ignore-errors source --legend ./target/debug/lcov.info && \
	open ./target/debug/coverage/index.html && \
	find ./target \( -name "jammdb*" \) -print | xargs rm -rf

docs:
	cargo +nightly doc

docs-open:
	cargo +nightly doc --open

test-32-bit:
	docker run --rm -v "$(PWD)":/usr/src/myapp -w /usr/src/myapp i386/rust:1.42.0 cargo test
