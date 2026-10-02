{
    description = "zenoh-web-relay: fan one zenoh-web backend out to many browsers (crate2nix builds, native and aarch64 / x86_64 Linux)";

    # zenoh-web's lib.crossRust: its nixpkgs / rust-overlay pins, so crates are shared with the other zenoh-web flakes
    inputs.zenoh-web.url = "github:jeff-hykin/zenoh-web";

    outputs = { self, zenoh-web }: {
        # zenoh-web-relay (native), zenoh-web-relay-aarch64-linux, zenoh-web-relay-x86_64-linux
        packages = zenoh-web.lib.eachSystem (system:
            let built = zenoh-web.lib.crossRustPackages { name = "zenoh-web-relay"; inherit system; cargoNix = ./Cargo.nix; };
            in built // { default = built.zenoh-web-relay; });
        devShells = zenoh-web.devShells;
    };
}
