import unittest
import re
from unittest import mock
from jinja2 import Environment, FileSystemLoader


class TestODPSMacros(unittest.TestCase):
    def setUp(self) -> None:
        self.jinja_env = Environment(
            loader=FileSystemLoader("dbt/include/odps/macros"),
            extensions=["jinja2.ext.do"],
        )

        self.config = {}
        self.default_context = {
            "validation": mock.Mock(),
            "model": mock.Mock(),
            "exceptions": mock.Mock(),
            "config": mock.Mock(),
            "adapter": mock.Mock(),
            "return": lambda r: r,
        }
        self.default_context["config"].get = lambda key, default=None, **kwargs: self.config.get(key, default)

    def __get_template(self, template_filename):
        return self.jinja_env.get_template(template_filename, globals=self.default_context)

    def __run_macro(self, template, name, relation, sql=None):
        self.default_context["model"].alias = relation

        def dispatch(macro_name, macro_namespace=None, packages=None):
            return getattr(template.module, f"odps__{macro_name}")

        self.default_context["adapter"].dispatch = dispatch

        value = getattr(template.module, f"odps__{name}")(False, relation, sql)
        return re.sub(r"\s\s+", " ", value)

    def test_macros_load(self):
        self.__get_template("adapters.sql")

    def test_macros_create_table_as(self):
        template = self.__get_template("adapters.sql")

        sql = self.__run_macro(template, "create_table_as", "my_table", "select 1").strip()
        self.assertEqual(sql, "create table if not exists my_table as select 1")

    def test_macros_create_table_with_lifecycle(self):
        template = self.__get_template("adapters.sql")

        self.config["lifecycle"] = 3
        sql = self.__run_macro(template, "create_table_as", "my_table", "select 1").strip()
        self.assertEqual(sql, "create table if not exists my_table lifecycle 3 as select 1")

    def test_macro_create_table_as_with_properties(self):
        template = self.__get_template("adapters.sql")

        self.config["properties"] = {"transactional": "true"}
        sql = self.__run_macro(template, "create_table_as", "my_table", "select 1").strip()
        self.assertEqual(
            sql, "create table if not exists my_table tblproperties('transactional'='true') as select 1"
        )

    def test_macro_create_table_as_with_partitioned_by_array_of_dict(self):
        template = self.__get_template("adapters.sql")

        self.config["partitioned_by"] = [{"col_name": "dt"}]
        sql = self.__run_macro(template, "create_table_as", "my_table", "select 1").strip()
        self.assertEqual(sql, "create table if not exists my_table partitioned by (dt string) as select 1")
