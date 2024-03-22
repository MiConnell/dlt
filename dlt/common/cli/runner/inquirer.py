import os
import typing as t
import textwrap as tw
import dlt

from dlt.cli import echo as fmt
from dlt.common.cli.runner.errors import FriendlyExit, PreflightError, RunnerError
from dlt.common.cli.runner.types import PipelineMembers, RunnerParams
from dlt.sources import DltResource, DltSource


dot_dlt = ".dlt"
select_message = """Please select your %s:
%s
"""


def make_select_options(
    select_what: str, items: t.Dict[str, t.Any]
) -> t.Tuple[str, t.List[str], t.List[str]]:
    options = []
    option_values = []
    choice_message = ""
    for idx, name in enumerate(items.keys()):
        choice_message += f"{idx} - {name}\n"
        options.append(str(idx))
        option_values.append(name)

    choice_message += "n - exit\n"
    options.append("n")
    return select_message % (select_what, choice_message), options, option_values


class Inquirer:
    """This class does pre flight checks required to run the pipeline.
    Also handles user input to allow users to select pipeline, resources and sources.
    """

    def __init__(self, params: RunnerParams, pipeline_members: PipelineMembers) -> None:
        self.params = params
        self.pipelines = pipeline_members.get("pipelines")
        self.sources = pipeline_members.get("sources")
        self.preflight_checks()

    def maybe_ask(self) -> t.Tuple[dlt.Pipeline, t.Union[DltResource, DltSource]]:
        """Shows prompts to select pipeline, resources and sources

        Returns:
            (DltResource, DltSource): a pair of pipeline and resources and sources
        """
        # save first pipeline and source
        pipeline_name, _ = next(iter(self.pipelines.items()))
        source_name, _ = next(iter(self.sources.items()))
        if self.params.pipeline_name:
            pipeline_name = self.params.pipeline_name
        elif len(self.pipelines) > 1:
            pipeline_name = self.ask_for_pipeline()

        if self.params.pipeline_name:
            source_name = self.params.pipeline_name
        elif len(self.sources) > 1:
            source_name = self.ask_for_source()

        pipeline = self.pipelines[pipeline_name]
        resource = self.sources[source_name]
        fmt.echo("Pipeline: " + fmt.style(pipeline_name, fg="blue", underline=True))

        if isinstance(resource, DltResource):
            label = "Resource"
        else:
            label = "Source"

        fmt.echo(f"{label}: " + fmt.style(source_name, fg="blue", underline=True))
        return pipeline, resource

    def ask_for_pipeline(self) -> str:
        if len(self.pipelines) > 1:
            message, options, values = make_select_options("pipeline", self.pipelines)

            choice = fmt.prompt(message, options, default="n")
            if choice == "n":
                raise FriendlyExit()

            pipeline_name = values[int(choice)]
            return pipeline_name

    def ask_for_source(self) -> str:
        message, options, values = make_select_options("resource or source", self.sources)

        choice = fmt.prompt(message, options, default="n")
        if choice == "n":
            raise FriendlyExit()

        source_name = values[int(choice)]
        return source_name

    def preflight_checks(self) -> None:
        if pipeline_name := self.params.pipeline_name:
            if pipeline_name not in self.pipelines:
                fmt.warning(f"Pipeline {pipeline_name} has not been found in pipeline script")
                raise PreflightError()

        if source_name := self.params.source_name:
            if source_name not in self.sources:
                fmt.warning(
                    f"Source or resouce with name: {source_name} has not been found in pipeline"
                    " script"
                )
                raise PreflightError()

        if self.params.current_dir != self.params.pipeline_workdir:
            fmt.warning(
                "Current working directory is different from the "
                f"pipeline script {self.params.pipeline_workdir}\n"
            )
            fmt.echo(f"Current workdir: {fmt.style(self.params.current_dir, fg='blue')}")
            fmt.echo(f"Pipeline workdir: {fmt.style(self.params.pipeline_workdir, fg='blue')}")

            has_cwd_config = self.has_dlt_config(self.params.current_dir)
            has_pipeline_config = self.has_dlt_config(self.params.pipeline_workdir)
            if has_cwd_config and has_pipeline_config:
                message = tw.dedent(
                    f"""
                    Found {dot_dlt} in current directory and pipeline directory if you intended to
                    use {self.params.pipeline_workdir}/{dot_dlt}, please change your current directory.

                    Using {dot_dlt} in current directory {self.params.current_dir}/{dot_dlt}.
                    """,
                )
                fmt.echo(fmt.warning_style(message))
            elif not has_cwd_config and has_pipeline_config:
                fmt.error(
                    f"{dot_dlt} is missing in current directory but exists in pipeline script's"
                    " directory"
                )
                fmt.info(
                    f"Please change your current directory to {self.params.pipeline_workdir} and"
                    " try again"
                )
                raise PreflightError()

    def check_if_runnable(self) -> None:
        if not self.pipelines:
            raise RunnerError(
                f"No pipeline instances found in pipeline script {self.params.script_path}"
            )

        if not self.sources:
            raise RunnerError(
                f"No source or resources found in pipeline script {self.params.script_path}"
            )

    def has_dlt_config(self, path: str) -> bool:
        return os.path.exists(os.path.join(path, ".dlt"))
