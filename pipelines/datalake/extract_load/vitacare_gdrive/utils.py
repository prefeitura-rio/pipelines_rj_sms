# -*- coding: utf-8 -*-
def fix_csv(csv_text: str, sep: str) -> str:
    first_line = csv_text.splitlines()[0]

    columns = first_line.split(sep)

    other_lines = csv_text.splitlines()[1:]

    max_cols = len(columns)
    for line in other_lines:
        line_columns = line.split(",")

        if len(line_columns) > max_cols:
            max_cols = len(line_columns)

    diff = max_cols - len(columns)

    for i in range(diff):
        columns.append(f"complemento_{i}")

    new_first_line = sep.join(columns)
    new_csv_text = new_first_line + "\n".join(other_lines)

    return new_csv_text
