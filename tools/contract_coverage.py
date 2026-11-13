"""Check that every adapter-contract clause names, for each implementer it binds, a test that exists.

docs/adapter-contract.md, section "Clause coverage", holds three tables: the implementers, the clauses
with the implementers each binds, and the coverage rows `| clause | implementer | test |`. Every clause
has a row for every implementer. A bound implementer's rows name tests as `<path>::<function>`, the path
from the repository root; an implementer the clause does not bind has a row `n/a: <reason>`. Each named
function must be defined in its file: `def test_...` for Python, a `#[test]` `fn` for Rust.

A test that may not run proves nothing on its own: a Python test with a pytest `xfail`, `skip` or `skipif`
mark (on the function, through a name it is decorated with, on its class, or in a `pytestmark` that
covers it) or a Rust test with an `#[ignore]` attribute does not satisfy its pair, and each bound pair
needs a row whose test runs unconditionally. A row naming an `xfail` test fails outright: it names a test
expected to fail. A Python test whose own body calls `pytest.skip`, `pytest.xfail` or `pytest.importorskip`
may not run either: its row gives the reason, as `<test> (runtime skip: <reason>)`, or the row fails.

The check reads source, so it does not see a skip or xfail called in a fixture, a mark a conftest hook
adds, or a Rust `#[cfg]` that compiles a test out.

Usage:
  contract_coverage.py [--root <repository>] [--doc <contract>]
Exit 0: complete; 1: a pair without a row, a test that is not there, a pair with no test that runs
unconditionally, an xfail test, a run-time skip without its reason, or a malformed table; 2: the
repository folder or the contract could not be read.
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOC = Path("docs") / "adapter-contract.md"
SECTION = "## Clause coverage"
CLAUSE_ID = re.compile(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*")
TEST_REF = re.compile(r"(?P<path>[^:\s]+)::(?P<name>[A-Za-z_][A-Za-z0-9_]*)")
NOT_APPLICABLE = re.compile(r"n/a:(?P<reason>.*)", re.DOTALL)
# A test cell's note that its test may skip itself at run time, and why; the cell's backticks are partly cut.
RUNTIME_SKIP = re.compile(r"`?\s*\(runtime skip:(?P<reason>[^)]*)\)$")


@dataclass(frozen=True)
class Row:
    clause: str
    implementer: str
    test: str
    skip_reason: str | None = None  # the row's `(runtime skip: <reason>)`, when it has one


@dataclass
class Contract:
    implementers: list[str] = field(default_factory=list)
    clauses: dict[str, list[str]] = field(default_factory=dict)
    rows: list[Row] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)


def _cells(line: str) -> list[str]:
    return [cell.strip().strip("`").strip() for cell in line.strip().strip("|").split("|")]


def _tables(text: str) -> dict[str, list[list[str]]]:
    """The body rows of each table under a `###` heading of the coverage section, by heading."""
    start = text.find(SECTION)
    if start < 0:
        return {}
    tables: dict[str, list[list[str]]] = {}
    heading = ""
    for line in text[start + len(SECTION) :].splitlines():
        if line.startswith("## "):
            break
        if line.startswith("### "):
            heading = line[4:].strip()
            tables[heading] = []
        elif line.lstrip().startswith("|") and heading:
            tables[heading].append(_cells(line))
    # The first row of each table is its header and the second its rule.
    return {name: rows[2:] for name, rows in tables.items()}


def parse(text: str) -> Contract:
    contract = Contract()
    tables = _tables(text)
    for name in ("Implementers", "Clauses", "Coverage"):
        if not tables.get(name):
            contract.problems.append(f'the "{SECTION[3:]}" section has no "{name}" table')
    for cells in tables.get("Implementers", []):
        name = cells[0]
        if name in contract.implementers:
            contract.problems.append(f"implementer {name} is listed twice")
        contract.implementers.append(name)
    for cells in tables.get("Clauses", []):
        if len(cells) < 2:
            contract.problems.append(f"clause row {cells} has no implementers")
            continue
        clause = cells[0]
        binds = [part.strip().strip("`") for part in cells[1].split(",") if part.strip()]
        if not CLAUSE_ID.fullmatch(clause):
            contract.problems.append(f"clause id {clause!r} is not lowercase words joined by hyphens")
        if clause in contract.clauses:
            contract.problems.append(f"clause {clause} is listed twice")
        if not binds:
            contract.problems.append(f"clause {clause} binds no implementer")
        for name in binds:
            if name not in contract.implementers:
                contract.problems.append(f"clause {clause} binds {name}, which is not a listed implementer")
        contract.clauses[clause] = binds
    for cells in tables.get("Coverage", []):
        if len(cells) != 3:
            contract.problems.append(f"coverage row {cells} does not have three cells")
            continue
        clause, implementer, test = cells
        note = RUNTIME_SKIP.search(test)
        if note is None or NOT_APPLICABLE.fullmatch(test):
            contract.rows.append(Row(clause, implementer, test))
        else:
            cut = test[: note.start()].strip().strip("`")
            contract.rows.append(Row(clause, implementer, cut, note["reason"].strip()))
    return contract


def structure_problems(contract: Contract) -> list[str]:
    """Every clause has a row for every implementer: a test where it binds, a reason where it does not."""
    problems = list(contract.problems)
    seen: dict[tuple[str, str], list[str]] = {}
    for row in contract.rows:
        where = f"{row.clause} x {row.implementer}"
        if row.clause not in contract.clauses:
            problems.append(f"{where}: no such clause")
            continue
        if row.implementer not in contract.implementers:
            problems.append(f"{where}: no such implementer")
            continue
        seen.setdefault((row.clause, row.implementer), []).append(row.test)
        if row.skip_reason == "":
            problems.append(f"{where}: a runtime skip needs a reason")
        bound = row.implementer in contract.clauses[row.clause]
        skipped = NOT_APPLICABLE.fullmatch(row.test)
        if skipped is not None:
            if not skipped["reason"].strip():
                problems.append(f"{where}: n/a needs a reason")
            if bound:
                problems.append(
                    f"{where}: the clause binds {row.implementer}, so the row names a test, not n/a"
                )
            continue
        found = TEST_REF.fullmatch(row.test)
        if found is None:
            problems.append(f"{where}: {row.test!r} is neither <path>::<function> nor n/a: <reason>")
        elif ".." in Path(found["path"]).parts or Path(found["path"]).is_absolute():
            problems.append(f"{where}: the test path leaves the repository")
        if not bound:
            problems.append(
                f"{where}: the clause does not bind {row.implementer}, so the row is n/a: <reason>"
            )
    for clause in contract.clauses:
        for name in contract.implementers:
            if (clause, name) not in seen:
                problems.append(f"{clause} x {name}: no row")
    return problems


CONDITIONAL_MARKS = frozenset({"xfail", "skip", "skipif"})
RUNTIME_SKIPS = frozenset({"skip", "xfail", "importorskip"})


@dataclass(frozen=True)
class Found:
    """A named test as its file defines it: whether it is there, why it may not run, and the pytest calls
    in its own body that end it early (`pytest.skip` and the like)."""

    defined: bool
    conditions: tuple[str, ...] = ()
    body_skips: tuple[str, ...] = ()


def _dotted(node: ast.AST) -> str:
    if isinstance(node, ast.Attribute):
        return f"{_dotted(node.value)}.{node.attr}"
    return node.id if isinstance(node, ast.Name) else ""


def _marks(node: ast.AST, names: Mapping[str, ast.expr]) -> set[str]:
    """The conditional pytest marks an expression applies: `<...>mark.xfail`, `skip` or `skipif`, called
    or not, inside a list or a parametrize, or through a module-level name bound to one."""
    found = set()
    for part in ast.walk(node):
        bound = names.get(part.id) if isinstance(part, ast.Name) else None
        for inner in [part] if bound is None else ast.walk(bound):
            if (
                isinstance(inner, ast.Attribute)
                and inner.attr in CONDITIONAL_MARKS
                and _dotted(inner.value).split(".")[-1] == "mark"
            ):
                found.add(inner.attr)
    return found


def _assigned(stmt: ast.stmt) -> list[tuple[str, ast.expr]]:
    """The plain names a statement binds, with the value bound."""
    if isinstance(stmt, ast.Assign):
        return [(t.id, stmt.value) for t in stmt.targets if isinstance(t, ast.Name)]
    if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name) and stmt.value is not None:
        return [(stmt.target.id, stmt.value)]
    return []


def _skip_calls(body: list[ast.stmt], tree: ast.Module) -> tuple[str, ...]:
    """The run-time skips `body` calls: `pytest.skip(`, `pytest.xfail(`, `pytest.importorskip(`, through
    the module's `import pytest as ...` or `from pytest import ...` names too."""
    modules: set[str] = {"pytest"}
    direct: dict[str, str] = {}
    for stmt in tree.body:
        if isinstance(stmt, ast.Import):
            modules |= {a.asname or a.name for a in stmt.names if a.name == "pytest"}
        elif isinstance(stmt, ast.ImportFrom) and stmt.module == "pytest":
            direct |= {a.asname or a.name: a.name for a in stmt.names if a.name in RUNTIME_SKIPS}
    found: set[str] = set()
    for node in (n for stmt in body for n in ast.walk(stmt)):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr in RUNTIME_SKIPS and _dotted(func.value) in modules:
            found.add(func.attr)
        elif isinstance(func, ast.Name) and func.id in direct:
            found.add(direct[func.id])
    return tuple(sorted(found))


def _python_test(source: str, name: str) -> Found:
    """The module- or class-level `def name` in `source`, with the conditional marks that cover it (its
    decorators, its classes' decorators, and each `pytestmark` of its module and classes) and the
    run-time skips its own body calls."""
    tree = ast.parse(source)
    names = {bound: value for stmt in tree.body for bound, value in _assigned(stmt)}

    def search(body: list[ast.stmt], covering: set[str]) -> Found | None:
        for stmt in body:
            for bound, value in _assigned(stmt):
                if bound == "pytestmark":
                    covering = covering | _marks(value, names)
        for stmt in body:
            if isinstance(stmt, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
                marks = covering.union(*(_marks(d, names) for d in stmt.decorator_list))
                if not isinstance(stmt, ast.ClassDef) and stmt.name == name:
                    return Found(True, tuple(sorted(marks)), _skip_calls(stmt.body, tree))
                inner = search(stmt.body, marks) if isinstance(stmt, ast.ClassDef) else None
                if inner is not None:
                    return inner
        return None

    return search(tree.body, set()) or Found(False)


def _rust_test(source: str, name: str) -> Found:
    """The `#[test]` fn `name` in `source`, and `ignore` when an attribute above it ignores it."""
    lines = source.splitlines()
    fn = re.compile(rf"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+{re.escape(name)}\s*[<(]")
    for index, line in enumerate(lines):
        if not fn.match(line):
            continue
        # The attributes and comments right above the function must hold its test attribute.
        attributes = []
        above = index - 1
        while above >= 0 and lines[above].strip().startswith(("#[", "//")):
            if lines[above].strip().startswith("#["):
                attributes.append(lines[above].strip())
            above -= 1
        if any(re.fullmatch(r"#\[(?:[a-z_]+::)?test\]", a) for a in attributes):
            ignored = any(re.search(r"\bignore\b", a) for a in attributes)
            return Found(True, ("ignore",) if ignored else ())
    return Found(False)


def _look_up(path: Path, name: str, sources: dict[Path, str | None]) -> tuple[Found, str]:
    """The named test in its file, and what is wrong with the reference ("" when nothing is)."""
    if path not in sources:
        try:
            sources[path] = path.read_text(encoding="utf-8")
        except OSError:
            sources[path] = None
    source = sources[path]
    if source is None:
        return Found(False), f"{path} cannot be read"
    if path.suffix not in (".py", ".rs"):
        return Found(False), "a test file is .py or .rs"
    if path.suffix == ".rs":
        test = _rust_test(source, name)
        return test, "" if test.defined else f"{path.name} has no a #[test] fn {name}"
    if not name.startswith("test_"):
        return Found(False), "a Python test's name starts with test_"
    try:
        test = _python_test(source, name)
    except SyntaxError as err:
        return Found(False), f"{path.name} cannot be parsed: {err.msg}"
    return test, "" if test.defined else f"{path.name} has no a def {name}"


def test_problems(contract: Contract, root: Path) -> list[str]:
    """Each named test is defined in its file under `root` and is not xfail, and each bound pair names
    one that runs unconditionally."""
    problems = []
    sources: dict[Path, str | None] = {}
    judged: dict[tuple[str, str], list[str]] = {}  # per pair, why each test may not run ("" when it runs)
    blind: set[tuple[str, str]] = set()  # pairs with a test that cannot be read
    for row in contract.rows:
        found = TEST_REF.fullmatch(row.test)
        if found is None:
            continue
        pair = (row.clause, row.implementer)
        where = f"{row.clause} x {row.implementer}: {row.test}"
        test, problem = _look_up(root / found["path"], found["name"], sources)
        if problem:
            problems.append(f"{where}: {problem}")
            blind.add(pair)
            continue
        if "xfail" in test.conditions:
            problems.append(f"{where}: marked xfail, so it is expected to fail and proves nothing")
        calls = ", ".join(f"pytest.{call}" for call in test.body_skips)
        if calls and row.skip_reason is None:
            problems.append(
                f"{where}: its body calls {calls}, so the row gives why: (runtime skip: <reason>)"
            )
        if row.skip_reason is not None and not calls:
            problems.append(f"{where}: the row gives a runtime skip its body does not make")
        conditions = [*test.conditions, f"{calls} in its body"] if calls else list(test.conditions)
        judged.setdefault(pair, []).append(f"{row.test} ({', '.join(conditions)})" if conditions else "")
    for (clause, implementer), conditions in judged.items():
        bound = implementer in contract.clauses.get(clause, [])
        if bound and (clause, implementer) not in blind and all(conditions):
            problems.append(
                f"{clause} x {implementer}: no row names a test that runs unconditionally: "
                + "; ".join(conditions)
            )
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, default=ROOT, help="the repository (default: this one)")
    parser.add_argument("--doc", type=Path, help="the contract (default: <root>/docs/adapter-contract.md)")
    args = parser.parse_args(argv)
    root: Path = args.root
    if not root.is_dir():
        print(f"contract-coverage: {root} is not a directory", file=sys.stderr)
        return 2
    doc = args.doc or root / DOC
    try:
        text = doc.read_text(encoding="utf-8")
    except OSError as err:
        print(f"contract-coverage: cannot read the contract {doc}: {err}", file=sys.stderr)
        return 2
    contract = parse(text)
    tests = [r for r in contract.rows if TEST_REF.fullmatch(r.test)]
    print(
        f"{doc}: {len(contract.clauses)} clauses, {len(contract.implementers)} implementers, "
        f"{len(contract.rows)} rows, {len(tests)} naming tests"
    )
    problems = structure_problems(contract) + test_problems(contract, root)
    for problem in problems:
        print(f"FAIL {problem}")
    if problems:
        print(f"contract-coverage: {len(problems)} problems")
        return 1
    print(
        "contract-coverage: every clause names a test for each implementer it binds, each test exists, "
        "and each bound pair has one that runs unconditionally"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
