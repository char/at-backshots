{
  description = "relay";

  outputs = { self, nixpkgs }: let
    pkgs = import nixpkgs { system = "x86_64-linux"; };
  in {
    devShells."x86_64-linux".default = pkgs.mkShell {
      buildInputs = [
        pkgs.rustup
        pkgs.stdenv
      ];
    };
  };
}

