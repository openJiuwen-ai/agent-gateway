"""Installed application extensions must not depend on the launch directory."""
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from agent_gateway.extensions import manager


def test_installed_swarm_extensions_resolve_outside_checkout(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    installed = tmp_path / 'site-packages' / 'jiuwenswarm'
    with patch('importlib.util.find_spec', return_value=SimpleNamespace(
        submodule_search_locations=[str(installed)]
    )):
        assert installed / 'extensions' in manager._extension_search_path_candidates('jiuwenswarm/extensions')


def test_missing_swarm_package_is_optional(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with patch('importlib.util.find_spec', return_value=None):
        assert manager._extension_search_path_candidates('jiuwenswarm/extensions') == [tmp_path / 'jiuwenswarm/extensions']


def test_custom_relative_and_absolute_paths_are_not_reinterpreted(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with patch('importlib.util.find_spec', side_effect=AssertionError('unexpected package discovery')):
        assert manager._extension_search_path_candidates('custom/plugins') == [tmp_path / 'custom/plugins']
        assert manager._extension_search_path_candidates(str(tmp_path)) == [tmp_path]
