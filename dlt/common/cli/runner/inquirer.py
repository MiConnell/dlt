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

    def maybe_ask(self) -> t.Tuple[dlt.Pipeline, t.Union[DltResource, DltSource]]:
        """Shows prompts to select pipeline, resources and sources

        Returns:
            (DltResource, DltSource): a pair of pipeline and resources and sources
        """
        self.preflight_checks()
        pipeline_name = self.get_pipeline_name()
        pipeline = self.pipelines[pipeline_name]
        real_pipeline_name = f" (Pipeline: {pipeline.pipeline_name})"

        source_name = self.get_source_name()
        resource = self.sources[source_name]
        if isinstance(resource, DltResource):
            label = "Resource"
        else:
            label = "Source"

        real_source_name = f" ({label}: {resource.name})"

        fmt.echo(
            "\nPipeline: "
            + fmt.style(pipeline_name + real_pipeline_name, fg="blue", underline=True)
        )

        fmt.echo(
            f"{label}: " + fmt.style(source_name + real_source_name, fg="blue", underline=True)
        )
        return pipeline, resource

    def get_pipeline_name(self) -> str:
        if self.params.pipeline_name:
            return self.params.pipeline_name

        if len(self.pipelines) > 1:
            message, options, values = make_select_options("pipeline", self.pipelines)
            choice = self.ask(message, options, default="n")
            pipeline_name = values[choice]
            return pipeline_name

        pipeline_name, _ = next(iter(self.pipelines.items()))
        return pipeline_name

    def get_source_name(self) -> str:
        if self.params.source_name:
            return self.params.source_name

        if len(self.sources) > 1:
            message, options, values = make_select_options("resource or source", self.sources)
            choice = self.ask(message, options, default="n")
            source_name = values[choice]
            return source_name

        source_name, _ = next(iter(self.sources.items()))
        return source_name

    def ask(self, message: str, options: t.List[str], default: t.Optional[str] = None) -> int:
        choice = fmt.prompt(message, options, default=default)
        if choice == "n":
            raise FriendlyExit()

        return int(choice)

    def preflight_checks(self) -> None:
        if pipeline_name := self.params.pipeline_name:
            if pipeline_name not in self.pipelines:
                fmt.echo(
                    fmt.error_style(
                        f"Pipeline {pipeline_name} has not been found in pipeline script"
                        "You can choose one of: "
                        + ", ".join(self.pipelines.keys())
                    )
                )
                raise PreflightError()

        if source_name := self.params.source_name:
            if source_name not in self.sources:
                fmt.echo(
                    fmt.error_style(
                        f"Source or resouce with name: {source_name} has not been found in pipeline"
                        " script. \n You can choose one of: "
                        + ", ".join(self.sources.keys())
                    )
                )
                raise PreflightError()

        if self.params.current_dir != self.params.pipeline_workdir:
            fmt.echo(f"\nCurrent workdir: {fmt.style(self.params.current_dir, fg='blue')}")
            fmt.echo(f"Pipeline workdir: {fmt.style(self.params.pipeline_workdir, fg='blue')}\n")

            fmt.echo(
                fmt.warning_style(
                    "Current working directory is different from the "
                    f"pipeline script {self.params.pipeline_workdir}"
                )
            )

            has_cwd_config = self.has_dlt_config(self.params.current_dir)
            has_pipeline_config = self.has_dlt_config(self.params.pipeline_workdir)
            if has_cwd_config and has_pipeline_config:
                message = tw.dedent(
                    f"""Using {dot_dlt} in current directory, if you intended to use """
                    f"""{self.params.pipeline_workdir}/{dot_dlt}, please change your current directory.""",
                )
                fmt.echo(fmt.warning_style(message))
            elif not has_cwd_config and has_pipeline_config:
                fmt.echo(
                    fmt.error_style(
                        f"{dot_dlt} is missing in current directory but exists in pipeline script's"
                        " directory"
                    )
                )
                fmt.echo(
                    fmt.info_style(
                        "Please change your current directory to"
                        f" {self.params.pipeline_workdir} and try again"
                    )
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
