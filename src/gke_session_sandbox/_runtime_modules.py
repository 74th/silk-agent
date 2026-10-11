"""Files of this package that run inside the Sandbox Pod (copied into the runtime image).

`scripts/build-session-runtime-image.sh` reads this list (run this file to print it), so the
image and the package cannot drift apart. The modules may import only the standard library,
`aiohttp`, `grpc`, `google.protobuf`, and each other (the image installs no more than that);
`tests/session_sandbox/test_package.py` checks it.
"""

# Modules copied as `<name>.py`.
RUNTIME_MODULES = (
    "commands",
    "errors",
    "files",
    "network_probe",
    "supervisor",
    "tracker",
)

# Packages copied as directories (generated sandboxd gRPC code).
RUNTIME_PACKAGES = ("_sandboxd_proto",)

# Third-party top-level modules the runtime image installs.
RUNTIME_THIRD_PARTY = ("aiohttp", "grpc", "google")

if __name__ == "__main__":
    for name in RUNTIME_MODULES:
        print(f"{name}.py")
    for name in RUNTIME_PACKAGES:
        print(f"{name}/")
