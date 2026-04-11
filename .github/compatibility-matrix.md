# Compatibility Matrix

This file is the checked-in preface for the nightly compatibility report produced by
`.github/workflows/compatibility-matrix.yml`.

## Scope

- Build the current ferrokinesis release binary.
- Run the existing SDK and KCL conformance suites against that binary on a nightly schedule or manual dispatch.
- Publish a markdown artifact summarizing suite outcomes for the selected ref.

## Limitations

- This is the first implementation slice for issue `#146`, not the final AWS side-by-side wire diff.
- The current multi-SDK suites still hardcode localhost endpoints or static test credentials, so a real AWS-backed dual-run needs follow-up changes in the per-suite harnesses.
- Until that lands, treat the generated report as a nightly compatibility canary rather than a definitive AWS parity score.
