import re
from string import Template
from textwrap import dedent, indent
from typing import Dict

import sark_registry as registry


def registry_doc() -> str:
    """Generate documentation for all columns in the registry"""
    entry = Template(
        dedent(
            """
    **$name** ($type)
        .. list-table:: constraints
           :widths: auto
           :align: left

    $constraints
    """
        )
    )
    nspaces = entry.template.strip().splitlines()[1].find("list-table")
    nosub_pat = re.compile(r" +\.\. list-table::.+\$constraints\n", re.DOTALL)

    row = Template("* - $key\n  - $val")  # 2 column row

    def lst_fmt(lst) -> str:
        if isinstance(lst, list):
            return ", ".join(lst) if lst else "..."
        else:
            return lst

    def fmt(schema: Dict) -> str:
        if "constraints" in schema:
            schema["constraints"] = "\n".join(
                indent(row.substitute(key=k, val=lst_fmt(v)), " " * nspaces)
                for k, v in schema["constraints"].items()
            )
        # substitute and clean up unsubstituted keys
        return re.sub(nosub_pat, "", entry.safe_substitute(schema))

    col_types = {
        "cols": "Value columns - ``cols``",
        "idxcols": "Index columns - ``idxcols``",
    }
    contents = [".. contents::"]
    for col_t, schemas in registry.getall().items():
        desc = col_types[col_t]
        contents += [desc, "-" * len(desc)]  # section heading
        contents += ["".join([fmt(schema) for schema in schemas])]
    return "\n".join(contents)


if __name__ == "__main__":
    print(registry_doc())
