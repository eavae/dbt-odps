import os
from dbt.config.project import PartialProject


class Obj:
    which = "blah"
    single_threaded = False


def profile_from_dict(profile, profile_name, cli_vars="{}"):
    from dbt.config import Profile
    from dbt.config.renderer import ProfileRenderer
    from dbt.config.utils import parse_cli_vars

    if not isinstance(cli_vars, dict):
        cli_vars = parse_cli_vars(cli_vars)

    renderer = ProfileRenderer(cli_vars)
    return Profile.from_raw_profile_info(
        profile,
        profile_name,
        renderer,
    )


def project_from_dict(project, profile, packages=None, selectors=None, cli_vars="{}"):
    from dbt.config.renderer import DbtProjectYamlRenderer
    from dbt.config.utils import parse_cli_vars

    if not isinstance(cli_vars, dict):
        cli_vars = parse_cli_vars(cli_vars)

    renderer = DbtProjectYamlRenderer(profile, cli_vars)

    project_root = project.pop("project-root", os.getcwd())

    partial = PartialProject.from_dicts(
        project_root=project_root,
        project_dict=project,
        packages_dict=packages,
        selectors_dict=selectors,
    )
    return partial.render(renderer)


def config_from_parts_or_dicts(project, profile, packages=None, selectors=None, cli_vars="{}"):
    from dbt.config import Project, Profile, RuntimeConfig
    from copy import deepcopy

    if isinstance(project, Project):
        profile_name = project.profile_name
    else:
        profile_name = project.get("profile")

    if not isinstance(profile, Profile):
        profile = profile_from_dict(
            deepcopy(profile),
            profile_name,
            cli_vars,
        )

    if not isinstance(project, Project):
        project = project_from_dict(
            deepcopy(project),
            profile,
            packages,
            selectors,
            cli_vars,
        )

    args = Obj()
    args.vars = cli_vars
    args.profile_dir = "/dev/null"
    return RuntimeConfig.from_parts(project=project, profile=profile, args=args)


def load_internal_manifest_macros(config, macro_hook=lambda m: None):
    from dbt.parser.manifest import ManifestLoader

    return ManifestLoader.load_macros(config, macro_hook)


def inject_plugin(plugin):
    from dbt.adapters.factory import FACTORY

    key = plugin.adapter.type()
    FACTORY.plugins[key] = plugin


def clear_plugin(plugin):
    from dbt.adapters.factory import FACTORY

    key = plugin.adapter.type()
    FACTORY.plugins.pop(key, None)
    FACTORY.adapters.pop(key, None)


def inject_adapter(value, plugin):
    """Inject the given adapter into the adapter factory, so your hand-crafted
    artisanal adapter will be available from get_adapter() as if dbt loaded it.
    """
    inject_plugin(plugin)
    from dbt.adapters.factory import FACTORY

    key = value.type()
    FACTORY.adapters[key] = value
