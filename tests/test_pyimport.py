import click.testing
from popit_relationship import pyimport


def test_app_succeeds():
    runner = click.testing.CliRunner()
    result = runner.invoke(pyimport.app)
    assert result.exit_code == 0


def test_sync_succeeds():
    runner = click.testing.CliRunner()
    result = runner.invoke(pyimport.sync)
    assert result.exit_code == 0
