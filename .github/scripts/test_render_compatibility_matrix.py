import json
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("render-compatibility-matrix.py")


class RenderCompatibilityMatrixTests(unittest.TestCase):
    def run_script(
        self,
        *,
        workflow_text: str,
        rows: list[dict[str, object]],
    ) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            template = root / "template.md"
            workflow = root / "compatibility-matrix.yml"
            results_dir = root / "results"
            output = root / "report.md"

            template.write_text("# Compatibility Matrix\n", encoding="utf-8")
            workflow.write_text(workflow_text, encoding="utf-8")
            results_dir.mkdir()

            for index, row in enumerate(rows):
                (results_dir / f"{index}-{row['slug']}.json").write_text(
                    json.dumps(row), encoding="utf-8"
                )

            return subprocess.run(
                [
                    "python3",
                    str(SCRIPT),
                    "--template",
                    str(template),
                    "--results-dir",
                    str(results_dir),
                    "--output",
                    str(output),
                    "--workflow",
                    str(workflow),
                    "--ref",
                    "abc123",
                ],
                capture_output=True,
                text=True,
                check=False,
            )

    def test_renders_when_all_expected_results_are_present(self) -> None:
        workflow_text = textwrap.dedent(
            """
            jobs:
              compatibility:
                strategy:
                  matrix:
                    include:
                      - name: Go v2
                        slug: go-sdk-v2
                      - name: Python (boto3)
                        slug: python-boto3
            """
        )
        rows = [
            {
                "suite": "Go v2",
                "slug": "go-sdk-v2",
                "language": "go",
                "directory": "tests/conformance/go-sdk-v2",
                "needs_dynamodb": False,
                "outcome": "success",
            },
            {
                "suite": "Python (boto3)",
                "slug": "python-boto3",
                "language": "python",
                "directory": "tests/conformance/python-boto3",
                "needs_dynamodb": False,
                "outcome": "failure",
            },
        ]

        result = self.run_script(workflow_text=workflow_text, rows=rows)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")

    def test_fails_when_expected_result_is_missing(self) -> None:
        workflow_text = textwrap.dedent(
            """
            jobs:
              compatibility:
                strategy:
                  matrix:
                    include:
                      - name: Go v2
                        slug: go-sdk-v2
                      - name: Python (boto3)
                        slug: python-boto3
            """
        )
        rows = [
            {
                "suite": "Go v2",
                "slug": "go-sdk-v2",
                "language": "go",
                "directory": "tests/conformance/go-sdk-v2",
                "needs_dynamodb": False,
                "outcome": "success",
            }
        ]

        result = self.run_script(workflow_text=workflow_text, rows=rows)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("incomplete compatibility result set", result.stderr)
        self.assertIn("missing: python-boto3", result.stderr)

    def test_fails_when_duplicate_result_slug_is_uploaded(self) -> None:
        workflow_text = textwrap.dedent(
            """
            jobs:
              compatibility:
                strategy:
                  matrix:
                    include:
                      - name: Go v2
                        slug: go-sdk-v2
            """
        )
        row = {
            "suite": "Go v2",
            "slug": "go-sdk-v2",
            "language": "go",
            "directory": "tests/conformance/go-sdk-v2",
            "needs_dynamodb": False,
            "outcome": "success",
        }

        result = self.run_script(workflow_text=workflow_text, rows=[row, row])

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("duplicate compatibility result slugs", result.stderr)

    def test_fails_when_row_outcome_is_empty(self) -> None:
        workflow_text = textwrap.dedent(
            """
            jobs:
              compatibility:
                strategy:
                  matrix:
                    include:
                      - name: Go v2
                        slug: go-sdk-v2
            """
        )
        row = {
            "suite": "Go v2",
            "slug": "go-sdk-v2",
            "language": "go",
            "directory": "tests/conformance/go-sdk-v2",
            "needs_dynamodb": False,
            "outcome": "",
        }

        result = self.run_script(workflow_text=workflow_text, rows=[row])

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid compatibility outcome", result.stderr)
        self.assertIn("go-sdk-v2", result.stderr)

    def test_fails_when_row_outcome_is_unknown(self) -> None:
        workflow_text = textwrap.dedent(
            """
            jobs:
              compatibility:
                strategy:
                  matrix:
                    include:
                      - name: Go v2
                        slug: go-sdk-v2
            """
        )
        row = {
            "suite": "Go v2",
            "slug": "go-sdk-v2",
            "language": "go",
            "directory": "tests/conformance/go-sdk-v2",
            "needs_dynamodb": False,
            "outcome": "unknown",
        }

        result = self.run_script(workflow_text=workflow_text, rows=[row])

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid compatibility outcome", result.stderr)
        self.assertIn("go-sdk-v2", result.stderr)

    def test_fails_when_row_outcome_is_missing(self) -> None:
        workflow_text = textwrap.dedent(
            """
            jobs:
              compatibility:
                strategy:
                  matrix:
                    include:
                      - name: Go v2
                        slug: go-sdk-v2
            """
        )
        row = {
            "suite": "Go v2",
            "slug": "go-sdk-v2",
            "language": "go",
            "directory": "tests/conformance/go-sdk-v2",
            "needs_dynamodb": False,
        }

        result = self.run_script(workflow_text=workflow_text, rows=[row])

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid compatibility outcome", result.stderr)
        self.assertIn("go-sdk-v2", result.stderr)

    def test_accepts_cancelled_and_skipped_rows(self) -> None:
        workflow_text = textwrap.dedent(
            """
            jobs:
              compatibility:
                strategy:
                  matrix:
                    include:
                      - name: Go v2
                        slug: go-sdk-v2
                      - name: Python (boto3)
                        slug: python-boto3
            """
        )
        rows = [
            {
                "suite": "Go v2",
                "slug": "go-sdk-v2",
                "language": "go",
                "directory": "tests/conformance/go-sdk-v2",
                "needs_dynamodb": False,
                "outcome": "cancelled",
            },
            {
                "suite": "Python (boto3)",
                "slug": "python-boto3",
                "language": "python",
                "directory": "tests/conformance/python-boto3",
                "needs_dynamodb": False,
                "outcome": "skipped",
            },
        ]

        result = self.run_script(workflow_text=workflow_text, rows=rows)

        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
