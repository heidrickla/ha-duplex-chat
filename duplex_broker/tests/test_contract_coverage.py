"""The adapter contract names, for each clause and implementer, the test that proves it."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
TOOL = ROOT / "tools" / "contract_coverage.py"


def load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("contract_coverage", TOOL)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # its dataclasses look their module up by name
    spec.loader.exec_module(module)
    return module


cc = load()


def contract() -> Any:
    return cc.parse((ROOT / "docs" / "adapter-contract.md").read_text(encoding="utf-8"))


def test_every_clause_has_a_row_for_every_implementer() -> None:
    parsed = contract()
    assert parsed.clauses and parsed.implementers
    assert cc.structure_problems(parsed) == []


def test_every_row_names_a_test_that_exists_and_the_tool_exits_0_on_this_tree() -> None:
    assert cc.test_problems(contract(), ROOT) == []
    assert cc.main([]) == 0


def test_the_tool_exits_1_with_one_row_removed(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    lines = (ROOT / "docs" / "adapter-contract.md").read_text(encoding="utf-8").splitlines(keepends=True)
    head = "| `task-notice-policy` | `duplex-claude` | `crates/"
    only = [i for i, line in enumerate(lines) if line.startswith(head)]
    assert len(only) == 1
    doc = tmp_path / "contract.md"
    doc.write_text("".join(lines[: only[0]] + lines[only[0] + 1 :]), encoding="utf-8")
    capsys.readouterr()
    assert cc.main(["--doc", str(doc)]) == 1
    assert "FAIL task-notice-policy x duplex-claude: no row" in capsys.readouterr().out


# The check itself, on a small contract -------------------------------------------------------------------

SMALL = """# Contract

## Clause coverage

### Implementers

| implementer | what |
|---|---|
| `one` | the first |
| `two` | the second |

### Clauses

| clause | binds | rule |
|---|---|---|
| `a-rule` | `one`, `two` | both do it |
| `b-rule` | `one` | only one does it |

### Coverage

| clause | implementer | test |
|---|---|---|
{rows}
"""
ROWS = [
    "| `a-rule` | `one` | `t/test_x.py::test_one_does_a` |",
    "| `a-rule` | `two` | `t/x_test.rs::two_does_a` |",
    "| `b-rule` | `one` | `t/test_x.py::test_one_does_b` |",
    "| `b-rule` | `two` | n/a: two never meets b |",
]


# Tests that may not run: an xfail, a skipif, a module covered by a skip mark, an ignored Rust test.
MAY_NOT_RUN_PY = """import pytest

ELSEWHERE = pytest.mark.skipif(True, reason="elsewhere")


@pytest.mark.xfail(strict=True, reason="a known fault")
def test_one_does_a_one_day():
    pass


@ELSEWHERE
def test_one_does_a_elsewhere():
    pass
"""
SKIPPED_MODULE_PY = """import pytest

pytestmark = [pytest.mark.skip(reason="off")]


def test_one_does_b_off():
    pass
"""
IGNORED_RS = '#[test]\n#[ignore = "needs the real broker"]\nfn two_does_a_with_the_broker() {}\n'
# Tests that may end themselves at run time, by a call in their own body.
SKIPS_ITSELF_PY = """import pytest
from pytest import xfail as known


def test_one_does_a_where_it_can():
    if True:
        pytest.skip("not here")


def test_one_does_a_some_day():
    def later():
        known("a known fault")

    later()
"""


def repo(tmp_path: Path) -> Path:
    (tmp_path / "t").mkdir(parents=True, exist_ok=True)
    (tmp_path / "t" / "test_x.py").write_text(
        "def test_one_does_a():\n    pass\n\n\nasync def test_one_does_b():\n    pass\n"
    )
    (tmp_path / "t" / "x_test.rs").write_text("#[test]\n// a comment\nfn two_does_a() {}\n\nfn helper() {}\n")
    (tmp_path / "t" / "test_maybe.py").write_text(MAY_NOT_RUN_PY)
    (tmp_path / "t" / "test_off.py").write_text(SKIPPED_MODULE_PY)
    (tmp_path / "t" / "maybe_test.rs").write_text(IGNORED_RS)
    (tmp_path / "t" / "test_itself.py").write_text(SKIPS_ITSELF_PY)
    return tmp_path


def problems(tmp_path: Path, rows: list[str]) -> list[str]:
    parsed = cc.parse(SMALL.format(rows="\n".join(rows)))
    out: list[str] = cc.structure_problems(parsed) + cc.test_problems(parsed, repo(tmp_path))
    return out


def test_a_whole_small_contract_passes(tmp_path: Path) -> None:
    assert problems(tmp_path, ROWS) == []


def test_a_pair_without_a_row_is_named(tmp_path: Path) -> None:
    assert problems(tmp_path, ROWS[:1] + ROWS[2:]) == ["a-rule x two: no row"]
    assert problems(tmp_path, ROWS[:3]) == ["b-rule x two: no row"]


def test_a_test_not_defined_in_its_file_is_named(tmp_path: Path) -> None:
    gone = [ROWS[0].replace("test_one_does_a", "test_gone"), *ROWS[1:]]
    assert problems(tmp_path, gone) == [
        "a-rule x one: t/test_x.py::test_gone: test_x.py has no a def test_gone"
    ]
    helper = [ROWS[0], ROWS[1].replace("two_does_a", "helper"), *ROWS[2:]]
    assert [p.split(": ")[-1] for p in problems(tmp_path, helper)] == ["x_test.rs has no a #[test] fn helper"]
    missing = [ROWS[0].replace("t/test_x.py", "t/test_y.py"), *ROWS[1:]]
    assert problems(tmp_path, missing)[0].endswith("cannot be read")


def test_n_a_needs_a_reason_and_only_where_the_clause_does_not_bind(tmp_path: Path) -> None:
    assert problems(tmp_path, [*ROWS[:3], "| `b-rule` | `two` | n/a:   |"]) == [
        "b-rule x two: n/a needs a reason"
    ]
    bound = [ROWS[0], "| `a-rule` | `two` | n/a: it is hard |", *ROWS[2:]]
    assert problems(tmp_path, bound) == [
        "a-rule x two: the clause binds two, so the row names a test, not n/a"
    ]
    unbound = [*ROWS[:3], "| `b-rule` | `two` | `t/x_test.rs::two_does_a` |"]
    assert problems(tmp_path, unbound) == [
        "b-rule x two: the clause does not bind two, so the row is n/a: <reason>"
    ]


XFAIL = "t/test_maybe.py::test_one_does_a_one_day"
SKIPIF = "t/test_maybe.py::test_one_does_a_elsewhere"
OFF = "t/test_off.py::test_one_does_b_off"
IGNORED = "t/maybe_test.rs::two_does_a_with_the_broker"


def test_an_xfail_row_alone_fails(tmp_path: Path) -> None:
    rows = [f"| `a-rule` | `one` | `{XFAIL}` |", *ROWS[1:]]
    assert problems(tmp_path, rows) == [
        f"a-rule x one: {XFAIL}: marked xfail, so it is expected to fail and proves nothing",
        f"a-rule x one: no row names a test that runs unconditionally: {XFAIL} (xfail)",
    ]
    # Beside a test that runs it still fails: a row never names a test expected to fail.
    assert problems(tmp_path, [*ROWS, f"| `a-rule` | `one` | `{XFAIL}` |"]) == [
        f"a-rule x one: {XFAIL}: marked xfail, so it is expected to fail and proves nothing"
    ]


def test_an_ignored_rust_row_alone_fails(tmp_path: Path) -> None:
    rows = [ROWS[0], f"| `a-rule` | `two` | `{IGNORED}` |", *ROWS[2:]]
    assert problems(tmp_path, rows) == [
        f"a-rule x two: no row names a test that runs unconditionally: {IGNORED} (ignore)"
    ]
    # Kept beside its twin that always runs, the ignored row is fine.
    assert problems(tmp_path, [*ROWS, f"| `a-rule` | `two` | `{IGNORED}` |"]) == []


def test_a_skipif_row_alone_fails(tmp_path: Path) -> None:
    rows = [f"| `a-rule` | `one` | `{SKIPIF}` |", *ROWS[1:]]
    assert problems(tmp_path, rows) == [
        f"a-rule x one: no row names a test that runs unconditionally: {SKIPIF} (skipif)"
    ]
    # A module's pytestmark covers each test in it.
    rows = [*ROWS[:2], f"| `b-rule` | `one` | `{OFF}` |", ROWS[3]]
    assert problems(tmp_path, rows) == [
        f"b-rule x one: no row names a test that runs unconditionally: {OFF} (skip)"
    ]


def test_a_missing_root_or_contract_exits_2_and_a_gap_exits_1(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    doc = tmp_path / "contract.md"
    doc.write_text(SMALL.format(rows="\n".join(ROWS[:3])), encoding="utf-8")
    here = repo(tmp_path / "dc")
    assert cc.main(["--root", str(tmp_path / "absent"), "--doc", str(doc)]) == 2
    assert "is not a directory" in capsys.readouterr().err
    assert cc.main(["--root", str(here), "--doc", str(tmp_path / "none.md")]) == 2
    assert "cannot read the contract" in capsys.readouterr().err
    assert cc.main(["--root", str(here), "--doc", str(doc)]) == 1
    assert "FAIL b-rule x two: no row" in capsys.readouterr().out
    doc.write_text(SMALL.format(rows="\n".join(ROWS)), encoding="utf-8")
    assert cc.main(["--root", str(here), "--doc", str(doc)]) == 0


def test_a_test_path_must_stay_inside_the_repository(tmp_path: Path) -> None:
    rows = [ROWS[0].replace("`t/test_x.py", "`../t/test_x.py"), *ROWS[1:]]
    assert any("leaves the repository" in p for p in problems(tmp_path, rows))


WHERE_IT_CAN = "t/test_itself.py::test_one_does_a_where_it_can"
SOME_DAY = "t/test_itself.py::test_one_does_a_some_day"


def test_a_test_that_skips_itself_needs_the_reason_on_its_row_and_never_stands_alone(tmp_path: Path) -> None:
    bare = [*ROWS, f"| `a-rule` | `one` | `{WHERE_IT_CAN}` |", f"| `a-rule` | `one` | `{SOME_DAY}` |"]
    assert problems(tmp_path, bare) == [
        f"a-rule x one: {WHERE_IT_CAN}: its body calls pytest.skip, so the row gives why: "
        "(runtime skip: <reason>)",
        f"a-rule x one: {SOME_DAY}: its body calls pytest.xfail, so the row gives why: "
        "(runtime skip: <reason>)",
    ]
    # With its reason the row is fine beside a test that always runs, and still fails alone.
    noted = f"| `a-rule` | `one` | `{WHERE_IT_CAN}` (runtime skip: needs the real app-server) |"
    assert problems(tmp_path, [*ROWS, noted]) == []
    assert problems(tmp_path, [noted, *ROWS[1:]]) == [
        f"a-rule x one: no row names a test that runs unconditionally: {WHERE_IT_CAN} "
        "(pytest.skip in its body)"
    ]
    assert problems(tmp_path, [*ROWS, noted.replace("needs the real app-server", " ")]) == [
        "a-rule x one: a runtime skip needs a reason"
    ]
    stale = "| `a-rule` | `one` | `t/test_x.py::test_one_does_a` (runtime skip: gone) |"
    assert problems(tmp_path, [stale, *ROWS[1:]]) == [
        "a-rule x one: t/test_x.py::test_one_does_a: the row gives a runtime skip its body does not make"
    ]
