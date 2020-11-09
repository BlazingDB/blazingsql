import collections
import json
import re

__all__ = ['get_json_plan']

def get_json_plan(algebra):
    lines = algebra.split("\n")
    lines = _replace_indentation_for_tabs(lines)
    lines = list(filter(None, lines))
    list_step = _lines_to_list_steps(lines)
    return _visit(list_step)


def _is_double_children(expr):
    return "LogicalJoin" in expr or "LogicalUnion" in expr


def _visit(lines):
    deque = collections.deque()
    root_level = 0
    dicc = {"expr": lines[root_level][1], "children": []}
    processed = set()
    for index in range(len(lines)):
        child_level, expr = lines[index]
        if child_level == root_level + 1:
            new_dicc = {"expr": expr, "children": []}
            if len(dicc["children"]) == 0: #No es necesario, deberia funcionar con solo append
                dicc["children"] = [new_dicc]
            else:
                dicc["children"].append(new_dicc)
            deque.append((index, child_level, expr, new_dicc))
            processed.add(index)

    for index in processed:
        lines[index][0] = -1

    while len(deque) > 0:
        curr_index, curr_level, curr_expr, curr_dicc = deque.pop()
        processed = set()

        if curr_index < len(lines) - 1:  # is brother
            child_level, expr = lines[curr_index + 1]
            if child_level == curr_level:
                continue
            elif child_level == curr_level + 1:
                index = curr_index + 1
                if _is_double_children(curr_expr):
                    while index < len(lines) and len(curr_dicc["children"]) < 2:
                        child_level, expr = lines[index]
                        if child_level == curr_level + 1:
                            new_dicc = {"expr": expr, "children": []}
                            if len(curr_dicc["children"]) == 0:
                                curr_dicc["children"] = [new_dicc]
                            else:
                                curr_dicc["children"].append(new_dicc)
                            processed.add(index)
                            deque.append((index, child_level, expr, new_dicc))
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
                            deque.append((index, child_level, expr, new_dicc))
                        index += 1

        for index in processed:
            lines[index][0] = -1
    return json.dumps(dicc)


def _validate_indendation(indentation_type, current_indentation):
    if not current_indentation:
        return

    match = re.search('^(' + indentation_type + ')+$', current_indentation)
    if not match:
        raise Exception(
        "Indentation invalid, current indentation is (" + indentation_type + "), but (" + current_indentation + ") was received.")


def _replace_indentation_for_tabs(lines):
    indentation_type = ''
    for i in range(len(lines)):
        lines[i] = lines[i].rstrip()
        match = re.search(r'(^\s+)(.*)',lines[i])

        if match:
            if not indentation_type:
                indentation_type = match.group(1)
            else:
                _validate_indendation(indentation_type, match.group(1))

            expr = match.group(2)
        else:
            expr = lines[i]

        beginning_spaces = len(lines[i]) - len(expr)
        if beginning_spaces > 0:
            lines[i] = ("\t" * (beginning_spaces // len(indentation_type) )) + expr

    return lines


def _lines_to_list_steps(lines):
    # It also removes the tabs in each expression
    list_step = []
    for i in range(len(lines)):
        line = lines[i]
        level = line.count("\t")
        list_step.append([level, line.replace("\t", "")])

    return list_step
