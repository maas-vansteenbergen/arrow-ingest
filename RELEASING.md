# Releasing arrow-ingest

## Version policy

Git tags follow [Semantic Versioning](https://semver.org/). The Python package
uses the corresponding [PEP 440](https://peps.python.org/pep-0440/) version.
For example, the Git tag `v0.1.0-alpha.1` is published as package version
`0.1.0a1`.

The project is in the `0.x` alpha series:

- Alpha releases are public and supported for experimentation, but their APIs
  and declarative configuration may change.
- Patch releases within a stable `0.MINOR` release line preserve documented
  behavior except where a security fix requires a change.
- A new `0.MINOR.0` release may introduce incompatible changes. Document
  every such change and its migration path in the changelog.
- Every release includes a changelog entry. Do not silently change a public
  contract.

## Release process

1. Select the next SemVer tag and its PEP 440 package-version equivalent.
2. Update `pyproject.toml`, regenerate `uv.lock`, and add a dated
   `CHANGELOG.md` entry with user-visible changes, breaking changes, security
   fixes, and known limitations.
3. Run the release checks:

   ```console
   just release-check
   ```

4. Merge the release commit into `main`.
5. Create and push an annotated tag from the merged `main` commit:

   ```console
   just release-tag v0.1.0-alpha.1
   ```

6. Create a GitHub Release from that tag, mark it as a pre-release when
   applicable, and use the matching changelog section as its release notes.
7. Publish the built package only through the project’s approved package
   publishing process. Verify the published version and its installation in a
   clean environment.
