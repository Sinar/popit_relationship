import click.testing
from popit_relationship import primport


def test_app_succeeds():
    runner = click.testing.CliRunner()
    result = runner.invoke(primport.app)
    assert result.exit_code == 0


def test_sync_succeeds():
    runner = click.testing.CliRunner()
    result = runner.invoke(primport.sync)
    assert result.exit_code == 0
