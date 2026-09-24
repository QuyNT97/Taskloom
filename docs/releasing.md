# Releasing

Task Engine uses one version for all three packages. Publish them in dependency
order: kernel, plugins, then core.

## Preflight

1. Confirm npm authentication with the `yuqgnort` account.
2. Ensure the working tree contains only the intended release changes.
3. Update every package version and the internal dependency versions together.
4. Update `CHANGELOG.md` with the release date and final changes.
5. Run a clean build and the complete test suite.
6. Inspect each package with `npm pack --dry-run`.

For v0.1.0, the verification commands are:

```sh
npm run check
npm run pack:check
```

## Publish

After the release commit and tag are ready, authenticate with npm and publish in
dependency order:

```sh
npm publish --workspace @yuqgnort/taskloom-kernel
npm publish --workspace @yuqgnort/taskloom-plugins
npm publish --workspace @yuqgnort/taskloom
```

Each package declares public access in `publishConfig`. Verify the registry
artifacts from a new temporary project before pushing the `v0.1.0` Git tag and
creating the GitHub release.
