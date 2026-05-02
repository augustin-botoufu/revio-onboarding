"""Unit tests for src/mode_dev.py (Jalon 5.0.2).

Tests the non-UI helpers: allowlist, slug, branch naming, YAML diff, and
the two orchestration funcs (generate_patch, open_pull_request) via fakes
for the Anthropic client and the github_sync module.

Run:
    cd /path/to/revio-onboarding && python -m pytest tests/test_mode_dev.py -v
or:
    python -m unittest tests/test_mode_dev.py -v
"""

from __future__ import annotations

import datetime as _dt
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

# Make ``src`` importable when running this file directly.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src import mode_dev as md
from src import github_sync as gh


# =============================================================================
# Allowlist
# =============================================================================

class TestAllowlist(unittest.TestCase):

    def test_default_allowlist_includes_founding_team(self):
        al = md.get_allowlist()
        self.assertIn("augustin@gorevio.co", al)
        self.assertIn("victor@gorevio.co", al)
        self.assertIn("adrien@gorevio.co", al)

    def test_is_allowlisted_case_insensitive(self):
        self.assertTrue(md.is_allowlisted("AUGUSTIN@gorevio.co"))
        self.assertTrue(md.is_allowlisted("  augustin@gorevio.co  "))
        self.assertFalse(md.is_allowlisted(""))
        self.assertFalse(md.is_allowlisted(None))
        self.assertFalse(md.is_allowlisted("randomguy@gmail.com"))

    def test_env_var_overrides_default(self):
        with patch.dict(os.environ, {"MODE_DEV_EMAILS": "foo@bar.com, baz@qux.io"}):
            al = md.get_allowlist()
            self.assertEqual(al, ["foo@bar.com", "baz@qux.io"])
            self.assertTrue(md.is_allowlisted("FOO@bar.com"))
            # The default allowlist is replaced, not merged.
            self.assertFalse(md.is_allowlisted("augustin@gorevio.co"))


# =============================================================================
# Slug + branch name
# =============================================================================

class TestSlug(unittest.TestCase):

    def test_slugify_strips_accents_and_special_chars(self):
        self.assertEqual(md._slugify("Pour les VASP force isHT=true"),
                         "pour-les-vasp-force-isht-true")

    def test_slugify_handles_empty(self):
        self.assertEqual(md._slugify(""), "patch")
        self.assertEqual(md._slugify("!!!"), "patch")

    def test_slugify_truncates(self):
        long = "a" * 100
        self.assertEqual(len(md._slugify(long, max_len=40)), 40)

    def test_build_branch_name_deterministic(self):
        when = _dt.datetime(2026, 4, 24, 14, 30, 0)
        name = md.build_branch_name("force isHT sur les VASP", now=when)
        self.assertEqual(name, "mode-dev/20260424-143000-force-isht-sur-les-vasp")

    def test_branch_name_prefixed(self):
        when = _dt.datetime(2026, 4, 24, 14, 30, 0)
        self.assertTrue(
            md.build_branch_name("anything", now=when).startswith("mode-dev/")
        )


# =============================================================================
# Diff + YAML validation
# =============================================================================

class TestDiffAndYaml(unittest.TestCase):

    def test_diff_reports_no_change(self):
        self.assertEqual(md.unified_diff("a\n", "a\n"), "(aucun changement)")

    def test_diff_shows_added_line(self):
        diff = md.unified_diff("a\nb\n", "a\nb\nc\n", filename="x.yml")
        self.assertIn("+c", diff)
        self.assertIn("a/x.yml", diff)
        self.assertIn("b/x.yml", diff)

    def test_validate_yaml_text_happy(self):
        ok, err = md.validate_yaml_text("foo: 1\nbar: [1, 2, 3]\n")
        self.assertTrue(ok)
        self.assertEqual(err, "")

    def test_validate_yaml_text_catches_bad(self):
        ok, err = md.validate_yaml_text("foo: [unterminated\n")
        self.assertFalse(ok)
        self.assertIn("YAML", err)


# =============================================================================
# Rules file listing
# =============================================================================

class TestListRulesFiles(unittest.TestCase):

    def test_skips_auto_managed_files(self):
        files = md.list_rules_files()
        names = {f.name for f in files}
        self.assertNotIn("learned_patterns.yml", names)
        self.assertNotIn("learned_columns.yml", names)
        self.assertNotIn("value_mappings.yml", names)
        # And contract.yml should be there (well-known).
        self.assertIn("contract.yml", names)

    def test_repo_paths_are_src_rules_slash_name(self):
        files = md.list_rules_files()
        for f in files:
            self.assertTrue(f.repo_path.startswith("src/rules/"))
            self.assertEqual(f.repo_path, f"src/rules/{f.name}")


# =============================================================================
# generate_patch with mocked Anthropic
# =============================================================================

class FakeToolBlock:
    """Mimics an ``anthropic.types.ToolUseBlock``."""
    def __init__(self, name, input_):
        self.type = "tool_use"
        self.name = name
        self.id = "fake_id"
        self.input = input_


class FakeTextBlock:
    def __init__(self, text):
        self.type = "text"
        self.text = text


class FakeResponse:
    def __init__(self, content, stop_reason="end_turn"):
        self.content = content
        self.stop_reason = stop_reason


class TestGeneratePatch(unittest.TestCase):

    def setUp(self):
        # Ensure env has a fake key so generate_patch gets past the gate.
        self.env_patch = patch.dict(os.environ, {"ANTHROPIC_API_KEY": "sk-fake"})
        self.env_patch.start()

    def tearDown(self):
        self.env_patch.stop()

    def _fake_anthropic(self, tool_input):
        """Build a fake Anthropic client that returns a forced tool call."""
        client = MagicMock()
        client.messages.create.return_value = FakeResponse(
            content=[FakeToolBlock("propose_yaml_patch", tool_input)],
        )
        return client

    def test_generate_patch_happy_path(self):
        fake_input = {
            "new_content": "foo: 2",
            "summary": "Bumps foo from 1 to 2",
            "risks": ["No known risks"],
            "commit_message": "rules/x: bump foo",
            "pr_title": "Mode Dev: bump foo",
            "pr_body": "Bumps foo.",
        }
        client = self._fake_anthropic(fake_input)
        with patch.object(md, "Anthropic", return_value=client):
            proposal = md.generate_patch(
                file_name="x.yml",
                current_yaml="foo: 1\n",
                user_request="monte foo à 2",
            )
        self.assertTrue(proposal.ok)
        self.assertEqual(proposal.new_content, "foo: 2\n")  # newline appended
        self.assertEqual(proposal.summary, "Bumps foo from 1 to 2")
        self.assertEqual(proposal.risks, ["No known risks"])
        self.assertEqual(proposal.commit_message, "rules/x: bump foo")

    def test_generate_patch_missing_api_key(self):
        # Force the SDK check to succeed (even if anthropic isn't installed in
        # the test env) so we exercise the api-key check next.
        with patch.object(md, "Anthropic", MagicMock()), \
             patch.dict(os.environ, {"ANTHROPIC_API_KEY": ""}, clear=False):
            os.environ.pop("ANTHROPIC_API_KEY", None)
            proposal = md.generate_patch(
                file_name="x.yml",
                current_yaml="",
                user_request="plop",
            )
        self.assertFalse(proposal.ok)
        self.assertIn("ANTHROPIC_API_KEY", proposal.error)

    def test_generate_patch_no_tool_call(self):
        """If Claude responds with text only (no forced tool), we report failure."""
        client = MagicMock()
        client.messages.create.return_value = FakeResponse(
            content=[FakeTextBlock("Désolé, je ne peux pas.")],
        )
        with patch.object(md, "Anthropic", return_value=client):
            proposal = md.generate_patch(
                file_name="x.yml",
                current_yaml="foo: 1",
                user_request="???",
            )
        self.assertFalse(proposal.ok)
        self.assertIn("pas produit de patch structuré", proposal.error)
        self.assertEqual(proposal.raw_text, "Désolé, je ne peux pas.")


# =============================================================================
# open_pull_request with mocked github_sync
# =============================================================================

class TestOpenPullRequest(unittest.TestCase):

    def setUp(self):
        self.fake_cfg = gh.GitHubConfig(
            token="t", repo="owner/repo", branch="main", path="x.yml",
        )
        self.proposal = md.PatchProposal(
            ok=True,
            new_content="foo: 2\n",
            summary="s",
            risks=[],
            commit_message="rules: bump",
            pr_title="Mode Dev: bump",
            pr_body="bumps foo.",
        )

    def test_happy_path_creates_branch_commits_and_opens_pr(self):
        when = _dt.datetime(2026, 4, 24, 10, 0, 0)

        with patch.object(gh, "get_branch_sha", return_value="deadbeef"), \
             patch.object(gh, "create_branch", return_value={}) as mock_branch, \
             patch.object(gh, "fetch_file_on_branch",
                          return_value=gh.RemoteFile(text="foo: 1\n", sha="abc")), \
             patch.object(gh, "commit_file_on_branch",
                          return_value={"commit": {"sha": "cafebabe"}}) as mock_commit, \
             patch.object(gh, "create_pull_request",
                          return_value={"html_url": "https://github.com/owner/repo/pull/42",
                                        "number": 42}) as mock_pr:
            result = md.open_pull_request(
                repo_path="src/rules/contract.yml",
                new_content="foo: 2\n",
                proposal=self.proposal,
                user_request="monte foo à 2",
                author_email="augustin@gorevio.co",
                cfg=self.fake_cfg,
                now=when,
            )

        self.assertTrue(result.ok, msg=result.error)
        self.assertEqual(result.commit_sha, "cafebabe")
        self.assertEqual(result.pr_url, "https://github.com/owner/repo/pull/42")
        self.assertEqual(result.pr_number, 42)
        self.assertTrue(result.branch.startswith("mode-dev/20260424-100000-"))

        # Branch created from main's sha.
        args, kwargs = mock_branch.call_args
        self.assertIn("from_sha", kwargs)
        self.assertEqual(kwargs["from_sha"], "deadbeef")

        # Commit on the fresh branch with the proposal's message.
        args, kwargs = mock_commit.call_args
        self.assertEqual(kwargs["branch"], result.branch)
        self.assertEqual(kwargs["sha"], "abc")
        self.assertEqual(kwargs["message"], "rules: bump")
        self.assertEqual(kwargs["author_email"], "augustin@gorevio.co")

        # PR opened head=branch, base=main, body includes the user request.
        args, kwargs = mock_pr.call_args
        self.assertEqual(kwargs["head"], result.branch)
        self.assertEqual(kwargs["title"], "Mode Dev: bump")
        self.assertIn("monte foo à 2", kwargs["body"])
        self.assertIn("augustin@gorevio.co", kwargs["body"])

    def test_missing_github_config_surfaces_friendly_error(self):
        with patch.object(gh, "get_config",
                          side_effect=gh.GitHubNotConfigured("missing GITHUB_TOKEN")):
            result = md.open_pull_request(
                repo_path="src/rules/contract.yml",
                new_content="foo: 2\n",
                proposal=self.proposal,
                user_request="x",
            )
        self.assertFalse(result.ok)
        self.assertIn("GITHUB_TOKEN", result.error)

    def test_commit_failure_reports_branch_already_created(self):
        with patch.object(gh, "get_branch_sha", return_value="deadbeef"), \
             patch.object(gh, "create_branch", return_value={}), \
             patch.object(gh, "fetch_file_on_branch",
                          return_value=gh.RemoteFile(text="foo: 1\n", sha="abc")), \
             patch.object(gh, "commit_file_on_branch",
                          side_effect=gh.GitHubSyncError("boom")):
            result = md.open_pull_request(
                repo_path="src/rules/contract.yml",
                new_content="foo: 2\n",
                proposal=self.proposal,
                user_request="x",
                cfg=self.fake_cfg,
            )
        self.assertFalse(result.ok)
        self.assertIn("Commit sur la branche échoué", result.error)
        self.assertTrue(result.branch.startswith("mode-dev/"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
