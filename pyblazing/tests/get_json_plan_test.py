from pyblazing.apiv2.algebra import get_json_plan
import pytest

@pytest.mark.parametrize("type_indentation", [' ','  ','   ','    ','\t','\t\t','\t\t\t','\t\t ',' \t','  \t'])
@pytest.mark.parametrize("multiple_line_break", ['','\n','\n\n','\n\n\n'])
@pytest.mark.parametrize("final_line_break", [True, False])
@pytest.mark.parametrize("multiple_tabs_each_line", ['','\t','\t\t','\t\t\t'])
@pytest.mark.parametrize("multiple_space_each_line", ['',' ','  ','   '])
def test_get_json_plan(type_indentation, multiple_line_break, final_line_break, multiple_tabs_each_line, multiple_space_each_line):
    algebra = ("LogicalJoin(condition=[=($6, $0)], joinType=[left]){1}{2}{3}\n"
               "{0}LogicalJoin(condition=[=($3, $1)], joinType=[left]){1}{2}{3}\n"
               "{0}{0}LogicalTableScan(table=[[main, product]]){1}{2}{3}\n"
               "{0}{0}LogicalTableScan(table=[[main, client]]){1}{2}{3}\n"
               "{0}LogicalTableScan(table=[[main, preference]]){1}{2}{3}")

    if final_line_break:
        algebra += "\n"

    algebra = algebra.format(type_indentation, multiple_tabs_each_line, multiple_space_each_line, multiple_line_break)

    result = get_json_plan(algebra)

    cmp = ('{'
           '"expr": "LogicalJoin(condition=[=($6, $0)], joinType=[left])", '
           '"children": ['
                       '{'
                       '"expr": "LogicalJoin(condition=[=($3, $1)], joinType=[left])", '
                       '"children": ['
                                   '{'
                                   '"expr": "LogicalTableScan(table=[[main, product]])", '
                                   '"children": []'
                                   '}, '
                                   '{'
                                   '"expr": "LogicalTableScan(table=[[main, client]])", '
                                   '"children": []'
                                   '}'
                                   ']'
                       '}, '
                       '{'
                       '"expr": "LogicalTableScan(table=[[main, preference]])", '
                       '"children": []'
                       '}'
                       ']'
           '}')

    assert cmp == result

def test_get_json_plan_throw_exception_case1():
    try:
        algebra = ("LogicalJoin(condition=[=($3, $1)], joinType=[left])"
                    " LogicalTableScan(table=[[main, product]])"
                    "\tLogicalTableScan(table=[[main, client]])")

        get_json_plan(algebra)

        assert 0

    except Exception:
        pass

def test_get_json_plan_throw_exception_case2():
    try:
        algebra = ("LogicalJoin(condition=[=($3, $1)], joinType=[left])"
                    "\tLogicalTableScan(table=[[main, product]])"
                    " LogicalTableScan(table=[[main, client]])")

        get_json_plan(algebra)

        assert 0

    except Exception:
        pass

def test_get_json_plan_throw_exception_case3():
    try:
        algebra = ("LogicalJoin(condition=[=($3, $1)], joinType=[left])"
                    "  LogicalTableScan(table=[[main, product]])"
                    "   LogicalTableScan(table=[[main, client]])")

        get_json_plan(algebra)

        assert 0

    except Exception:
        pass

def test_get_json_plan_throw_exception_case4():
    try:
        algebra = ("LogicalJoin(condition=[=($3, $1)], joinType=[left])"
                    " \tLogicalTableScan(table=[[main, product]])"
                    "\t LogicalTableScan(table=[[main, client]])")

        get_json_plan(algebra)

        assert 0

    except Exception:
        pass