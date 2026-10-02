# Exact compiler/sysroot environment used for the retained measurements.
export PATH=/nix/store/61h9aazfpc0iwqlcbkq1w86v20529c0h-cargo-1.96.0-x86_64-unknown-linux-gnu/bin:/nix/store/rj5p4dpyr4xrwfwl21m948pfk2c9akpd-rustc-1.96.0-x86_64-unknown-linux-gnu/bin:/nix/store/0j1ajvl2qwwb9n5a91hzd0j98fk9fa3k-gcc-wrapper-14.3.0/bin:/nix/store/05xfwnl62nipbw1ankbcv2crjhb9a918-python3-3.13.14/bin:$PATH
export RUSTFLAGS='--sysroot=/nix/store/wg64by85cy7816f2aysllgjgc71qmj9q-rust-std-1.96.0-x86_64-unknown-linux-gnu'
export RUSTDOCFLAGS="$RUSTFLAGS"
export PATH=/nix/store/jwfr8731byzqdhizax9k6paa00zn2yis-git-with-svn-2.54.0/bin:$PATH
