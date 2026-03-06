import click.testing
from pathlib import Path

from popit_relationship import primport


def test_app_succeeds():
    runner = click.testing.CliRunner()
    result = runner.invoke(primport.app)
    assert result.exit_code == 0


def test_sync_succeeds():
    runner = click.testing.CliRunner()
    result = runner.invoke(primport.sync)
    assert result.exit_code == 0


def test_export_succeeds():
    runner = click.testing.CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(primport.app, ["export", "graphml", "test.graphml"])
        assert result.exit_code == 0
        assert Path("test.graphml").exists()
