{
    description = "zenoh-gateway-relay: fan one zenoh-gateway backend out to many browsers (crate2nix builds, native and aarch64 / x86_64 Linux)";

    # zenoh-gateway's lib.crossRust: its nixpkgs / rust-overlay pins, so crates are shared with the other zenoh-gateway flakes
    inputs.zenoh-gateway.url = "github:jeff-hykin/zenoh-gateway";

    outputs = { self, zenoh-gateway }: {
        # zenoh-gateway-relay (native), zenoh-gateway-relay-aarch64-linux, zenoh-gateway-relay-x86_64-linux
        packages = zenoh-gateway.lib.eachSystem (system:
            let built = zenoh-gateway.lib.crossRustPackages { name = "zenoh-gateway-relay"; inherit system; cargoNix = ./Cargo.nix; };
            in built // { default = built.zenoh-gateway-relay; });
        devShells = zenoh-gateway.devShells;
    };
}
