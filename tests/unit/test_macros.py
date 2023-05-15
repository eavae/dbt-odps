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

    def __run_macro_clustered_by_clause(self):
        template = self.__get_template("adapters.sql")
        return getattr(template.module, "odps__clustered_by_clause")()

    def __run_macro_create_table_as(self, relation, sql=None):
        self.jinja_env.get_template("adapters.sql", globals=self.default_context)
        template = self.__get_template("relations/create_relation.sql")
        value = getattr(template.module, "odps__create_table_as")(False, relation, sql)
        return re.sub(r"\s\s+", " ", value)

    def test_macros_load(self):
        self.__get_template("adapters.sql")

    def test_macros_create_table_as(self):
        self.default_context["lifecycle_clause"] = lambda temporary: ""

        sql = self.__run_macro_create_table_as("my_table", "select 1").strip()

        self.assertEqual(sql, "create table if not exists my_table as select 1")

    def test_macros_create_table_with_lifecycle(self):
        self.default_context["lifecycle_clause"] = lambda temporary: "lifecycle 3"

        sql = self.__run_macro_create_table_as("my_table", "select 1").strip()

        self.assertEqual(sql, "create table if not exists my_table lifecycle 3 as select 1")

    def test_macro_clustered_by_clause(self):
        # test single column
        self.config["clustered_by"] = ["col1"]
        sql = self.__run_macro_clustered_by_clause().strip()
        self.assertEqual(sql, "clustered by (col1)")

        # test multiple columns
        self.config["clustered_by"] = ["col1", "col2"]
        sql = self.__run_macro_clustered_by_clause().strip()
        self.assertEqual(sql, "clustered by (col1, col2)")

        # test empty
        self.config["clustered_by"] = []
        sql = self.__run_macro_clustered_by_clause().strip()
        self.assertEqual(sql, "")

        # test range clustered by
        self.config["range_clustered_by"] = ["col1"]
        sql = self.__run_macro_clustered_by_clause().strip()
        self.assertEqual(sql, "range clustered by (col1)")

        # test sorted by when no clustered by
        self.config = {}
        self.config["sorted_by"] = ["col1"]
        sql = self.__run_macro_clustered_by_clause().strip()
        self.assertEqual(sql, "")

        # test sorted by when clustered by
        self.config = {}
        self.config["clustered_by"] = ["col1"]
        self.config["sorted_by"] = ["col1", {"col_name": "col2", "order": "desc"}]
        sql = self.__run_macro_clustered_by_clause().strip()
        self.assertEqual(sql, "clustered by (col1)\n  sorted by (col1, col2 desc)")

        # test number of buckets when no clustered by
        self.config = {}
        self.config["number_of_buckets"] = 10
        sql = self.__run_macro_clustered_by_clause().strip()
        self.assertEqual(sql, "")

        # test number of buckets when clustered by
        self.config = {}
        self.config["clustered_by"] = ["col1"]
        self.config["number_of_buckets"] = 10
        sql = self.__run_macro_clustered_by_clause().strip()
        self.assertEqual(sql, "clustered by (col1)\n  \n  \n  into 10 buckets")
