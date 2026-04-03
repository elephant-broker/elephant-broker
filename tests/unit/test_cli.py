"""Tests for CLI commands — server (elephantbroker) and admin (ebrun)."""
from click.testing import CliRunner


class TestServerCLI:
    """Tests for elephantbroker server CLI (server.py)."""

    def test_serve_command_exists(self):
        from elephantbroker.server import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["serve", "--help"])
        assert result.exit_code == 0
        assert "Start the ElephantBroker" in result.output

    def test_health_check_command_exists(self):
        from elephantbroker.server import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["health-check", "--help"])
        assert result.exit_code == 0

    def test_migrate_command_exists(self):
        from elephantbroker.server import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["migrate"])
        assert result.exit_code == 0
        assert "No migrations needed" in result.output

    def test_serve_default_port(self):
        from elephantbroker.server import serve
        for param in serve.params:
            if param.name == "port":
                assert param.default == 8420

    def test_health_check_default_port(self):
        from elephantbroker.server import health_check
        for param in health_check.params:
            if param.name == "port":
                assert param.default == 8420

    def test_serve_has_config_option(self):
        from elephantbroker.server import serve
        param_names = [p.name for p in serve.params]
        assert "config" in param_names


class TestEbrunCLI:
    """Tests for ebrun admin CLI (cli.py)."""

    def test_bootstrap_command_exists(self):
        from elephantbroker.cli import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["bootstrap", "--help"])
        assert result.exit_code == 0
        assert "org-name" in result.output
        assert "team-name" in result.output
        assert "admin-name" in result.output

    def test_org_create_command_exists(self):
        from elephantbroker.cli import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["org", "create", "--help"])
        assert result.exit_code == 0
        assert "--name" in result.output

    def test_org_list_command_exists(self):
        from elephantbroker.cli import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["org", "list", "--help"])
        assert result.exit_code == 0

    def test_team_create_command_exists(self):
        from elephantbroker.cli import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["team", "create", "--help"])
        assert result.exit_code == 0
        assert "--org-id" in result.output

    def test_team_add_member_command_exists(self):
        from elephantbroker.cli import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["team", "add-member", "--help"])
        assert result.exit_code == 0

    def test_actor_create_command_exists(self):
        from elephantbroker.cli import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["actor", "create", "--help"])
        assert result.exit_code == 0
        assert "--display-name" in result.output
        assert "--authority-level" in result.output

    def test_actor_merge_command_exists(self):
        from elephantbroker.cli import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["actor", "merge", "--help"])
        assert result.exit_code == 0

    def test_profile_list_command_exists(self):
        from elephantbroker.cli import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["profile", "list", "--help"])
        assert result.exit_code == 0

    def test_profile_resolve_command_exists(self):
        from elephantbroker.cli import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["profile", "resolve", "--help"])
        assert result.exit_code == 0

    def test_authority_list_command_exists(self):
        from elephantbroker.cli import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["authority", "list", "--help"])
        assert result.exit_code == 0

    def test_authority_set_command_exists(self):
        from elephantbroker.cli import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["authority", "set", "--help"])
        assert result.exit_code == 0
        assert "--min-level" in result.output

    def test_goal_create_command_exists(self):
        from elephantbroker.cli import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["goal", "create", "--help"])
        assert result.exit_code == 0
        assert "--scope" in result.output

    def test_goal_list_command_exists(self):
        from elephantbroker.cli import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["goal", "list", "--help"])
        assert result.exit_code == 0

    def test_config_set_command_exists(self):
        from elephantbroker.cli import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["config", "set", "--help"])
        assert result.exit_code == 0

    def test_config_show_command_exists(self):
        from elephantbroker.cli import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["config", "show"])
        assert result.exit_code == 0

    def test_actor_id_flag_accepted(self):
        from elephantbroker.cli import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["--actor-id", "test-uuid", "authority", "list", "--help"])
        assert result.exit_code == 0


class TestConfigLoading:
    """Tests for YAML config loading."""

    def test_from_yaml_loads_file(self, tmp_path):
        from elephantbroker.schemas.config import ElephantBrokerConfig
        yaml_content = "gateway:\n  gateway_id: test-gw\ninfra:\n  log_level: debug\n"
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml_content)
        config = ElephantBrokerConfig.from_yaml(str(config_file))
        assert config.gateway.gateway_id == "test-gw"
        assert config.infra.log_level == "debug"

    def test_from_yaml_env_overrides(self, tmp_path, monkeypatch):
        from elephantbroker.schemas.config import ElephantBrokerConfig
        yaml_content = "gateway:\n  gateway_id: yaml-gw\n"
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml_content)
        monkeypatch.setenv("EB_GATEWAY_ID", "env-gw")
        config = ElephantBrokerConfig.from_yaml(str(config_file))
        assert config.gateway.gateway_id == "env-gw"

    def test_from_yaml_invalid_raises(self, tmp_path):
        from elephantbroker.schemas.config import ElephantBrokerConfig
        config_file = tmp_path / "bad.yaml"
        config_file.write_text("invalid: [yaml: {broken")
        import pytest
        with pytest.raises(Exception):
            ElephantBrokerConfig.from_yaml(str(config_file))

    def test_from_yaml_missing_file_raises(self):
        from elephantbroker.schemas.config import ElephantBrokerConfig
        import pytest
        with pytest.raises(FileNotFoundError):
            ElephantBrokerConfig.from_yaml("/nonexistent/path.yaml")

    def test_from_env_still_works(self):
        from elephantbroker.schemas.config import ElephantBrokerConfig
        config = ElephantBrokerConfig.from_env()
        assert config.gateway.gateway_id is not None

    def test_default_yaml_loads(self):
        import os
        from elephantbroker.schemas.config import ElephantBrokerConfig
        yaml_path = os.path.join(os.path.dirname(__file__), "..", "..", "elephantbroker", "config", "default.yaml")
        if os.path.exists(yaml_path):
            config = ElephantBrokerConfig.from_yaml(yaml_path)
            assert config.gateway.gateway_id == "local"
