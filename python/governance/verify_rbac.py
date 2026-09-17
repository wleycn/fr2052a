# [AI-GENERATED] model=deepseek-flash date=2026-09-17 reviewed_by=pending
"""RBAC 权限自测 —— 逐角色实际连库读一次，而不是看授权语句写没写。

为什么必须实跑：
    授权语句执行成功不等于权限符合预期。GRANT 写错对象、忘了 REVOKE PUBLIC、
    给了角色但角色没生效 —— 这些都只在真读一次的时候才暴露。
    「我 GRANT 过了」不是证据，「以该角色身份读成功/被拒绝」才是。

做法：以建库账号连库，用 SET ROLE 切换角色，逐个对象试读，
与下表声明的预期逐一比对。失败要报 FAIL，不该读到的读到了同样报 FAIL。

用法（Server 2，用 venv 里的 python 直接跑）：
    ./venv/bin/python /opt/fr2052a-app/python/governance/verify_rbac.py
"""

from __future__ import annotations

import os
import sys

import psycopg2

# (角色, 对象, 是否应可读)。这是权限设计的声明，不是实现 ——
# 实现（GRANT）在 sql/postgres/20_security.sql，两边不一致时本脚本会红。
EXPECTATIONS: tuple[tuple[str, str, bool], ...] = (
    ("fr2052a_admin", "ads.ads_fr2052a_report", True),
    ("fr2052a_admin", "audit.audit_change_log", True),
    ("fr2052a_admin", "secure.fr2052a_pii_map", True),
    ("compliance_officer", "ads.ads_fr2052a_report", True),
    ("compliance_officer", "audit.audit_change_log", True),
    ("compliance_officer", "secure.fr2052a_pii_map", True),
    ("treasury_analyst", "ads.ads_fr2052a_report", True),
    ("treasury_analyst", "audit.audit_change_log", False),
    ("treasury_analyst", "secure.fr2052a_pii_map", False),
    ("auditor", "ads.ads_fr2052a_report", True),
    ("auditor", "audit.audit_change_log", True),
    ("auditor", "secure.fr2052a_pii_map", False),
)


def pg_connection() -> psycopg2.extensions.connection:
    """连接 Server 1 的 PostgreSQL。"""
    return psycopg2.connect(
        host=os.environ["SERVER1_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def try_read(connection: psycopg2.extensions.connection, role: str, obj: str) -> tuple[bool, str]:
    """以指定角色试读一个对象。返回 (是否读到了, 说明)。"""
    connection.rollback()
    with connection.cursor() as cursor:
        cursor.execute(f"SET ROLE {role}")
        try:
            # 采一行就够，目的是验证权限而不是取数据
            cursor.execute(f"SELECT 1 FROM {obj} LIMIT 1")
            rows = cursor.fetchall()
        except psycopg2.errors.InsufficientPrivilege as error:
            return False, str(error).strip().splitlines()[0][:80]
        except psycopg2.errors.UndefinedTable as error:
            return False, f"对象不存在：{str(error).strip().splitlines()[0][:60]}"
        except psycopg2.Error as error:
            return False, f"其他错误：{type(error).__name__}"
        finally:
            connection.rollback()
    return True, f"读到 {len(rows)} 行"


def main() -> int:
    """以每个角色的身份实读三张对象，把结果与预期权限逐条比对。"""
    connection = pg_connection()
    failures: list[str] = []
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT rolname FROM pg_roles "
                "WHERE rolname IN ('fr2052a_admin','compliance_officer','treasury_analyst','auditor')"
            )
            existing = {row[0] for row in cursor.fetchall()}
        connection.rollback()

        print("RBAC 权限自测（逐角色实读）")
        current_role = None
        for role, obj, expected in EXPECTATIONS:
            if role not in existing:
                print(f"  [FAIL] 角色 {role} 不存在，先应用 sql/postgres/20_security.sql")
                failures.append(role)
                continue
            if role != current_role:
                print(f"\n  {role}")
                current_role = role
            allowed, note = try_read(connection, role, obj)
            ok = allowed == expected
            verdict = "OK  " if ok else "FAIL"
            expectation = "应可读" if expected else "应拒绝"
            print(f"    [{verdict}] {obj:<44} {expectation}　实际{'可读' if allowed else '拒绝'}（{note}）")
            if not ok:
                failures.append(f"{role} -> {obj}")
    finally:
        connection.close()

    print()
    if failures:
        print(f"权限不符合预期 {len(failures)} 处：{failures}")
        return 1
    print(f"完成：{len(EXPECTATIONS)} 项权限全部符合预期")
    return 0


if __name__ == "__main__":
    sys.exit(main())
