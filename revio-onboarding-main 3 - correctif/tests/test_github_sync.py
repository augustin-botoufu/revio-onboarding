"""Unit tests for src/github_sync.py — Jalon 2.5.

Run from the project root with:

    python -m unittest tests.test_github_sync -v

Or to run a single test:

    python -m unittest tests.test_github_sync.TestUpsert.test_idempotent -v

No network: every HTTP call is mocked. These tests are the regression
safety net for the auto-commit flow — if a refactor breaks GitHub sync,
they fail fast before hitting Streamlit.
"""

from __future__ import annotations

import base64
import io
import json
import os
import sys
import unittest
from unittest import mock
from urllib.error import HTTPError

# Make src/ importable when running from repo root.
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src import github_sync as gh  # noqa: E402
from src import learned_patterns as lp  # noqa: E402


# --- Helpers ---------------------------------------------------------------

def _fake_cfg(repo: str = "octocat/Hello-World") -> gh.GitHubConfig:
    return gh.GitHubConfig(
        token="ghp_fake",
        repo=repo,
        branch="main",
        path="src/rules/learned_patterns.yml",
    )


def _http_response(body: dict) -> mock.MagicMock:
    """Build a mock for `urlopen(...)` to return a JSON body."""
    raw = json.dumps(body).encode("utf-8")
    resp = mock.MagicMock()
    resp.read.return_value = raw
    resp.__enter__.return_value = resp
    resp.__exit__.return_value = False
    return resp


def _http_error(code: int, body: dict | str = "") -> HTTPError:
    if isinstance(body, dict):
        body_bytes = json.dumps(body).encode("utf-8")
    else:
        body_bytes = body.encode("utf-8")
    return HTTPError(
        url="https://api.github.com/fake",
        code=code,
        msg="fake",
        hdrs=None,  # type: ignore[arg-type]
        fp=io.BytesIO(body_bytes),
    )


# --- Pure-Python logic (no HTTP) -------------------------------------------

class TestUpsert(unittest.TestCase):
    def test_insert_into_empty(self) -> None:
        entry = lp.build_pattern_entry(
            slug="autre_loueur_etat_parc",
            filename="alphabet.csv",
            columns=["Immatriculation", "VIN"],
            column_mapping={"registrationPlate": "Immatriculation"},
        )
        text = "# header\npatterns: []\n"
        out = gh.upsert_pattern(text, entry)
        self.assertIn("# header", out, "header must be preserved")
        self.assertIn("alphabet", out.lower(), "pattern must be inserted")

    def test_replace_same_id(self) -> None:
        e1 = lp.build_pattern_entry(
            slug="x", filename="foo.csv", columns=["A"],
            loueur_hint="v1",
        )
        e2 = dict(e1)
        e2["loueur_hint"] = "v2"
        text = "patterns: []\n"
        text = gh.upsert_pattern(text, e1)
        text = gh.upsert_pattern(text, e2)
        import yaml
        data = yaml.safe_load(text)
        self.assertEqual(len(data["patterns"]), 1, "same id must not duplicate")
        self.assertEqual(data["patterns"][0]["loueur_hint"], "v2")

    def test_idempotent(self) -> None:
        entry = lp.build_pattern_entry(
            slug="x", filename="foo.csv", columns=["A"],
        )
        text = "patterns: []\n"
        t1 = gh.upsert_pattern(text, entry)
        t2 = gh.upsert_pattern(t1, entry)
        self.assertEqual(t1, t2, "upserting the same entry twice is a no-op")

    def test_missing_id_raises(self) -> None:
        with self.assertRaises(gh.GitHubSyncError):
            gh.upsert_pattern("patterns: []\n", {"slug": "x"})

    def test_preserves_other_keys(self) -> None:
        """If the YAML grows extra root keys later, upsert mustn't drop them."""
        text = "patterns: []\nversion: 42\n"
        entry = lp.build_pattern_entry(slug="x", filename="f.csv", columns=["A"])
        out = gh.upsert_pattern(text, entry)
        import yaml
        data = yaml.safe_load(out)
        self.assertEqual(data.get("version"), 42)


class TestRemove(unittest.TestCase):
    def setUp(self) -> None:
        e1 = lp.build_pattern_entry(slug="x", filename="a.csv", columns=["A"])
        e2 = lp.build_pattern_entry(slug="x", filename="b.csv", columns=["B"])
        self.e1, self.e2 = e1, e2
        text = "patterns: []\n"
        text = gh.upsert_pattern(text, e1)
        text = gh.upsert_pattern(text, e2)
        self.text = text

    def test_remove_existing(self) -> None:
        new_text, removed = gh.remove_pattern(self.text, self.e1["id"])
        self.assertTrue(removed)
        import yaml
        data = yaml.safe_load(new_text)
        self.assertEqual(len(data["patterns"]), 1)
        self.assertEqual(data["patterns"][0]["id"], self.e2["id"])

    def test_remove_missing_is_noop(self) -> None:
        new_text, removed = gh.remove_pattern(self.text, "nope")
        self.assertFalse(removed)
        self.assertEqual(new_text, self.text)

    def test_remove_empty_id(self) -> None:
        new_text, removed = gh.remove_pattern(self.text, "")
        self.assertFalse(removed)
        self.assertEqual(new_text, self.text)


# --- Config & is_configured ------------------------------------------------

class TestConfig(unittest.TestCase):
    def setUp(self) -> None:
        # Ensure env is clean for each test.
        for k in ("GITHUB_TOKEN", "GITHUB_REPO", "GITHUB_BRANCH", "GITHUB_PATH"):
            os.environ.pop(k, None)

    def test_not_configured_when_empty(self) -> None:
        self.assertFalse(gh.is_configured())
        with self.assertRaises(gh.GitHubNotConfigured) as ctx:
            gh.get_config()
        self.assertIn("GITHUB_TOKEN", ctx.exception.user_message)

    def test_configured_from_env(self) -> None:
        os.environ["GITHUB_TOKEN"] = "ghp_abc"
        os.environ["GITHUB_REPO"] = "me/repo"
        self.assertTrue(gh.is_configured())
        cfg = gh.get_config()
        self.assertEqual(cfg.repo, "me/repo")
        self.assertEqual(cfg.branch, "main")  # default
        self.assertEqual(cfg.path, "src/rules/learned_patterns.yml")

    def test_invalid_repo_format(self) -> None:
        os.environ["GITHUB_TOKEN"] = "ghp_abc"
        os.environ["GITHUB_REPO"] = "notaslug"
        with self.assertRaises(gh.GitHubNotConfigured):
            gh.get_config()


# --- HTTP error translation ------------------------------------------------

class TestHttpErrorTranslation(unittest.TestCase):
    """Every HTTPError code should map to the right GitHubSyncError subclass."""

    def _call_with_error(self, code: int, body: dict | str = ""):
        err = _http_error(code, body)
        with mock.patch("src.github_sync.urlopen", side_effect=err):
            return gh._http("GET", "https://api.github.com/x", token="t")

    def test_404_maps_to_file_not_found(self) -> None:
        with self.assertRaises(gh.GitHubFileNotFound) as ctx:
            self._call_with_error(404, {"message": "Not Found"})
        self.assertEqual(ctx.exception.status_code, 404)

    def test_409_maps_to_conflict(self) -> None:
        with self.assertRaises(gh.GitHubConflict):
            self._call_with_error(409, {"message": "is at xyz but expected abc"})

    def test_422_sha_maps_to_conflict(self) -> None:
        with self.assertRaises(gh.GitHubConflict):
            self._call_with_error(
                422, {"message": "sha wasn't supplied correctly"}
            )

    def test_401_actionable_message(self) -> None:
        with self.assertRaises(gh.GitHubSyncError) as ctx:
            self._call_with_error(401, {"message": "Bad credentials"})
        self.assertEqual(ctx.exception.status_code, 401)
        self.assertIn("expiré", ctx.exception.user_message.lower())

    def test_403_actionable_message(self) -> None:
        with self.assertRaises(gh.GitHubSyncError) as ctx:
            self._call_with_error(403, {"message": "Forbidden"})
        self.assertEqual(ctx.exception.status_code, 403)

    def test_other_code_generic(self) -> None:
        with self.assertRaises(gh.GitHubSyncError) as ctx:
            self._call_with_error(500, {"message": "Boom"})
        self.assertEqual(ctx.exception.status_code, 500)
        # Must NOT be any of the specific subclasses.
        self.assertNotIsInstance(ctx.exception, gh.GitHubFileNotFound)
        self.assertNotIsInstance(ctx.exception, gh.GitHubConflict)


# --- fetch_patterns_yaml ---------------------------------------------------

class TestFetch(unittest.TestCase):
    def test_fetch_existing_file(self) -> None:
        yaml_text = "patterns:\n  - id: foo\n    slug: bar\n"
        encoded = base64.b64encode(yaml_text.encode("utf-8")).decode("ascii")
        body = {"content": encoded, "sha": "abc123"}
        with mock.patch("src.github_sync.urlopen", return_value=_http_response(body)):
            remote = gh.fetch_patterns_yaml(_fake_cfg())
        self.assertEqual(remote.text, yaml_text)
        self.assertEqual(remote.sha, "abc123")

    def test_fetch_missing_file_returns_seed(self) -> None:
        """404 must NOT raise — it seeds a default empty-patterns file."""
        err = _http_error(404, {"message": "Not Found"})
        with mock.patch("src.github_sync.urlopen", side_effect=err):
            remote = gh.fetch_patterns_yaml(_fake_cfg())
        self.assertEqual(remote.sha, "", "empty sha signals 'create on PUT'")
        self.assertIn("patterns: []", remote.text)

    def test_fetch_forwards_other_errors(self) -> None:
        err = _http_error(500, {"message": "Boom"})
        with mock.patch("src.github_sync.urlopen", side_effect=err):
            with self.assertRaises(gh.GitHubSyncError):
                gh.fetch_patterns_yaml(_fake_cfg())


# --- commit_file body shape ------------------------------------------------

class TestCommitFile(unittest.TestCase):
    def _captured_body(self, sha: str) -> dict:
        """Call commit_file under mock and return the JSON body sent to urlopen."""
        captured = {}

        def fake_urlopen(req, timeout=20):
            captured["method"] = req.get_method()
            captured["url"] = req.full_url
            captured["body"] = json.loads(req.data.decode("utf-8"))
            return _http_response({"commit": {"sha": "new"}})

        with mock.patch("src.github_sync.urlopen", side_effect=fake_urlopen):
            gh.commit_file(
                _fake_cfg(),
                "hello\n",
                sha=sha,
                message="test commit",
                author_email="me@example.com",
            )
        return captured

    def test_update_includes_sha(self) -> None:
        cap = self._captured_body(sha="existing_sha")
        self.assertEqual(cap["method"], "PUT")
        self.assertEqual(cap["body"].get("sha"), "existing_sha")

    def test_create_omits_sha(self) -> None:
        cap = self._captured_body(sha="")
        self.assertNotIn(
            "sha", cap["body"],
            "when creating a new file, sha must be omitted (else GitHub 422)",
        )

    def test_body_encodes_content_base64(self) -> None:
        cap = self._captured_body(sha="x")
        decoded = base64.b64decode(cap["body"]["content"]).decode("utf-8")
        self.assertEqual(decoded, "hello\n")

    def test_committer_set_when_author_provided(self) -> None:
        cap = self._captured_body(sha="x")
        self.assertEqual(
            cap["body"]["committer"]["email"], "me@example.com"
        )


# --- save_pattern end-to-end (happy + retry) -------------------------------

class TestSavePattern(unittest.TestCase):
    def setUp(self) -> None:
        os.environ["GITHUB_TOKEN"] = "ghp_x"
        os.environ["GITHUB_REPO"] = "me/repo"

    def tearDown(self) -> None:
        for k in ("GITHUB_TOKEN", "GITHUB_REPO"):
            os.environ.pop(k, None)

    def _entry(self) -> dict:
        return lp.build_pattern_entry(
            slug="autre_loueur_etat_parc",
            filename="alphabet.csv",
            columns=["Immatriculation"],
            column_mapping={"registrationPlate": "Immatriculation"},
        )

    def test_happy_path_create(self) -> None:
        """File doesn't exist → fetch returns empty sha → commit creates it."""
        calls: list[tuple[str, dict | None]] = []

        def fake_urlopen(req, timeout=20):
            method = req.get_method()
            if method == "GET":
                calls.append(("GET", None))
                # 404 → fetch seeds an empty file
                raise _http_error(404, {"message": "Not Found"})
            if method == "PUT":
                body = json.loads(req.data.decode("utf-8"))
                calls.append(("PUT", body))
                return _http_response({"commit": {"sha": "newsha"}})
            raise AssertionError(f"unexpected method {method}")

        with mock.patch("src.github_sync.urlopen", side_effect=fake_urlopen):
            resp = gh.save_pattern(self._entry())
        self.assertNotIn("skipped", resp)
        methods = [m for m, _ in calls]
        self.assertEqual(methods, ["GET", "PUT"])
        put_body = calls[1][1]
        assert put_body is not None
        self.assertNotIn("sha", put_body, "creating a new file = no sha")

    def test_skip_when_no_change(self) -> None:
        """Upserting the same pattern twice → second call is a no-op."""
        entry = self._entry()
        existing_yaml = (
            "patterns:\n"
            + "  - id: " + entry["id"] + "\n"
            + "    slug: " + entry["slug"] + "\n"
            + "    match:\n"
            + "      filename_regex: " + entry["match"]["filename_regex"] + "\n"
        )

        def fake_urlopen(req, timeout=20):
            # All GETs return the "already saved" state.
            encoded = base64.b64encode(
                gh.upsert_pattern(existing_yaml, entry).encode()
            ).decode("ascii")
            return _http_response({"content": encoded, "sha": "abc"})

        with mock.patch("src.github_sync.urlopen", side_effect=fake_urlopen):
            resp = gh.save_pattern(entry)
        self.assertTrue(resp.get("skipped"))

    def test_retry_on_conflict(self) -> None:
        """One 409 followed by success → save_pattern must re-fetch & retry."""
        n_puts = {"count": 0}

        def fake_urlopen(req, timeout=20):
            method = req.get_method()
            if method == "GET":
                # Empty file each time.
                encoded = base64.b64encode(b"patterns: []\n").decode("ascii")
                return _http_response({"content": encoded, "sha": "sha1"})
            if method == "PUT":
                n_puts["count"] += 1
                if n_puts["count"] == 1:
                    raise _http_error(409, {"message": "sha conflict"})
                return _http_response({"commit": {"sha": "ok"}})
            raise AssertionError

        with mock.patch("src.github_sync.urlopen", side_effect=fake_urlopen):
            resp = gh.save_pattern(self._entry())
        self.assertNotIn("skipped", resp)
        self.assertEqual(n_puts["count"], 2, "first PUT conflicts, second succeeds")

    def test_gives_up_after_max_retries(self) -> None:
        def fake_urlopen(req, timeout=20):
            if req.get_method() == "GET":
                encoded = base64.b64encode(b"patterns: []\n").decode("ascii")
                return _http_response({"content": encoded, "sha": "sha1"})
            raise _http_error(409, {"message": "still conflicting"})

        with mock.patch("src.github_sync.urlopen", side_effect=fake_urlopen):
            with self.assertRaises(gh.GitHubSyncError) as ctx:
                gh.save_pattern(self._entry())
        self.assertIn("concurrentes", ctx.exception.user_message)


# --- delete_pattern --------------------------------------------------------

class TestDeletePattern(unittest.TestCase):
    def setUp(self) -> None:
        os.environ["GITHUB_TOKEN"] = "ghp_x"
        os.environ["GITHUB_REPO"] = "me/repo"

    def tearDown(self) -> None:
        for k in ("GITHUB_TOKEN", "GITHUB_REPO"):
            os.environ.pop(k, None)

    def test_not_found_is_skipped(self) -> None:
        def fake_urlopen(req, timeout=20):
            if req.get_method() == "GET":
                encoded = base64.b64encode(b"patterns: []\n").decode("ascii")
                return _http_response({"content": encoded, "sha": "sha1"})
            raise AssertionError("no PUT expected — pattern doesn't exist")

        with mock.patch("src.github_sync.urlopen", side_effect=fake_urlopen):
            resp = gh.delete_pattern("nonexistent")
        self.assertTrue(resp.get("skipped"))
        self.assertEqual(resp.get("reason"), "not_found")

    def test_deletes_existing(self) -> None:
        entry = lp.build_pattern_entry(
            slug="x", filename="foo.csv", columns=["A"]
        )
        text = gh.upsert_pattern("patterns: []\n", entry)
        put_bodies: list[dict] = []

        def fake_urlopen(req, timeout=20):
            if req.get_method() == "GET":
                encoded = base64.b64encode(text.encode()).decode("ascii")
                return _http_response({"content": encoded, "sha": "sha1"})
            put_bodies.append(json.loads(req.data.decode("utf-8")))
            return _http_response({"commit": {"sha": "ok"}})

        with mock.patch("src.github_sync.urlopen", side_effect=fake_urlopen):
            resp = gh.delete_pattern(entry["id"])
        self.assertNotIn("skipped", resp)
        self.assertEqual(len(put_bodies), 1)
        new_yaml = base64.b64decode(put_bodies[0]["content"]).decode("utf-8")
        self.assertNotIn(entry["id"], new_yaml)


# --- check_connection ------------------------------------------------------

class TestCheckConnection(unittest.TestCase):
    def setUp(self) -> None:
        for k in ("GITHUB_TOKEN", "GITHUB_REPO", "GITHUB_BRANCH", "GITHUB_PATH"):
            os.environ.pop(k, None)

    def test_reports_not_configured(self) -> None:
        result = gh.check_connection()
        self.assertFalse(result["ok"] if "ok" in result else True)
        self.assertFalse(result.get("configured"))

    def test_reports_file_exists_and_count(self) -> None:
        os.environ["GITHUB_TOKEN"] = "t"
        os.environ["GITHUB_REPO"] = "me/r"
        yaml_text = (
            "patterns:\n"
            "  - {id: a, slug: x, match: {filename_regex: 'a'}}\n"
            "  - {id: b, slug: x, match: {filename_regex: 'b'}}\n"
        )
        encoded = base64.b64encode(yaml_text.encode()).decode("ascii")
        with mock.patch(
            "src.github_sync.urlopen",
            return_value=_http_response({"content": encoded, "sha": "abc"}),
        ):
            result = gh.check_connection()
        self.assertTrue(result.get("ok"))
        self.assertTrue(result.get("file_exists"))
        self.assertEqual(result.get("patterns_count"), 2)

    def test_reports_file_missing_gracefully(self) -> None:
        os.environ["GITHUB_TOKEN"] = "t"
        os.environ["GITHUB_REPO"] = "me/r"
        with mock.patch(
            "src.github_sync.urlopen",
            side_effect=_http_error(404, {"message": "Not Found"}),
        ):
            result = gh.check_connection()
        self.assertTrue(result.get("ok"), "404 on file is not an error — it's a create-on-save signal")
        self.assertFalse(result.get("file_exists"))
        self.assertIn("pas encore", result.get("message", ""))

    def test_reports_401_as_ok_false(self) -> None:
        os.environ["GITHUB_TOKEN"] = "t"
        os.environ["GITHUB_REPO"] = "me/r"
        with mock.patch(
            "src.github_sync.urlopen",
            side_effect=_http_error(401, {"message": "Bad credentials"}),
        ):
            result = gh.check_connection()
        self.assertFalse(result.get("ok"))
        self.assertIn("expiré", result.get("message", "").lower())


if __name__ == "__main__":
    unittest.main()
