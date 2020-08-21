from pyblazing.apiv2 import DataType

import collections
import json

# Util functions related to the Algebra


def modifyAlgebraForDataframesWithOnlyWantedColumns(
    algebra, tableScanInfo, originalTables
):
    for table_name in tableScanInfo:
        # TODO: handle situation with multiple tables being joined twice
        if originalTables[table_name].fileType == DataType.ARROW:
            orig_scan = tableScanInfo[table_name]["table_scans"][0]
            orig_col_indexes = tableScanInfo[table_name]["table_columns"][0]
            merged_col_indexes = list(range(len(orig_col_indexes)))

            new_col_indexes = []
            if len(merged_col_indexes) > 0:
                if orig_col_indexes == merged_col_indexes:
                    new_col_indexes = list(range(0, len(orig_col_indexes)))
                else:
                    enumerated_indexes = enumerate(merged_col_indexes)
                    for new_index, merged_col_index in enumerated_indexes:
                        if merged_col_index in orig_col_indexes:
                            new_col_indexes.append(new_index)

            orig_project = "projects=[" + str(orig_col_indexes) + "]"
            new_project = "projects=[" + str(new_col_indexes) + "]"
            new_scan = orig_scan.replace(orig_project, new_project)
            algebra = algebra.replace(orig_scan, new_scan)
    return algebra


def is_double_children(expr):
    return "LogicalJoin" in expr or "LogicalUnion" in expr


def visit(lines):
    stack = collections.deque()
    root_level = 0
    dicc = {"expr": lines[root_level][1], "children": []}
    processed = set()
    for index in range(len(lines)):
        child_level, expr = lines[index]
        if child_level == root_level + 1:
            new_dicc = {"expr": expr, "children": []}
            if len(dicc["children"]) == 0:
                dicc["children"] = [new_dicc]
            else:
                dicc["children"].append(new_dicc)
            stack.append((index, child_level, expr, new_dicc))
            processed.add(index)

    for index in processed:
        lines[index][0] = -1

    while len(stack) > 0:
        curr_index, curr_level, curr_expr, curr_dicc = stack.pop()
        processed = set()

        if curr_index < len(lines) - 1:  # is brother
            child_level, expr = lines[curr_index + 1]
            if child_level == curr_level:
                continue
            elif child_level == curr_level + 1:
                index = curr_index + 1
                if is_double_children(curr_expr):
                    while index < len(lines) and len(curr_dicc["children"]) < 2:
                        child_level, expr = lines[index]
                        if child_level == curr_level + 1:
                            new_dicc = {"expr": expr, "children": []}
                            if len(curr_dicc["children"]) == 0:
                                curr_dicc["children"] = [new_dicc]
                            else:
                                curr_dicc["children"].append(new_dicc)
                            processed.add(index)
                            stack.append((index, child_level, expr, new_dicc))
                        index += 1
                else:
                    while index < len(lines) and len(curr_dicc["children"]) < 1:
                        child_level, expr = lines[index]
                        if child_level == curr_level + 1:
                            new_dicc = {"expr": expr, "children": []}
                            if len(curr_dicc["children"]) == 0:
                                curr_dicc["children"] = [new_dicc]
                            else:
                                curr_dicc["children"].append(new_dicc)
                            processed.add(index)
                            stack.append((index, child_level, expr, new_dicc))
                        index += 1

        for index in processed:
            lines[index][0] = -1
    return json.dumps(dicc)


def get_plan(algebra):
    algebra = algebra.replace("  ", "\t")
    lines = algebra.split("\n")
    # algebra plan was provided and only contains one-line as logical plan
    if len(lines) == 1:
        algebra += "\n"
        lines = algebra.split("\n")
    new_lines = []
    for i in range(len(lines) - 1):
        line = lines[i]
        level = line.count("\t")
        new_lines.append([level, line.replace("\t", "")])
    return visit(new_lines)
