import pytest
from IPython.terminal.interactiveshell import TerminalInteractiveShell


@pytest.fixture(scope="session")
def shell_interactive():
    shell = TerminalInteractiveShell()
    shell.run_cell("import dlt")
    shell.run_cell("from dlt.cli.magics import register_notebook_magics")
    shell.run_cell("register_notebook_magics()")
    return shell


def test_init_command(shell_interactive):
    result = shell_interactive.run_line_magic(
        "init", "--source_name=chess --destination_name=duckdb"
    )
    # Check if the init command returns the expected result
    assert result == 0


def test_settings_command(shell_interactive):
    result = shell_interactive.run_line_magic("settings", "--enable-telemetry --debug")
    # Check if the init command returns the expected result
    assert result == 0


def test_list_pipeline_command(shell_interactive):
    result = shell_interactive.run_line_magic("pipeline", "--operation list-pipelines")
    # Check if the init command returns the expected result
    assert result == 0


@pytest.mark.parametrize(
    "operation", ["info", "trace", "failed-jobs", "drop-pending-packages", "schema"]
)
def test_operation_pipeline_command(shell_interactive, operation):
    result = shell_interactive.run_line_magic(
        "pipeline", f"--operation {operation} --pipeline_name chess"
    )
    # Check if the init command returns the expected result
    assert result == 0


def test_version_command(shell_interactive):
    result = shell_interactive.run_line_magic(
        "dlt_version", "dlt_version"
    )
    # Check if the init command returns the expected result
    assert result == 0
