require_relative "test_helper"

class RefTest < GitgresTest
  def test_create_ref
    oid = "aa" * 20  # 40 hex chars
    result = @conn.exec_params(
      "SELECT git_ref_update($1, 'refs/heads/main', decode($2, 'hex'))",
      [@repo_id, oid]
    )
    assert_equal "t", result[0]["git_ref_update"]

    # Verify it's stored
    result = @conn.exec_params(
      "SELECT encode(oid, 'hex') AS oid FROM refs WHERE repo_id = $1 AND name = 'refs/heads/main'",
      [@repo_id]
    )
    assert_equal oid, result[0]["oid"]
  end

  def test_update_ref_with_cas
    old_oid = "aa" * 20
    new_oid = "bb" * 20

    # Create
    @conn.exec_params("SELECT git_ref_update($1, 'refs/heads/main', decode($2, 'hex'))", [@repo_id, old_oid])

    # Update with correct old value
    result = @conn.exec_params(
      "SELECT git_ref_update($1, 'refs/heads/main', decode($2, 'hex'), decode($3, 'hex'))",
      [@repo_id, new_oid, old_oid]
    )
    assert_equal "t", result[0]["git_ref_update"]
  end

  def test_update_ref_cas_fails
    old_oid = "aa" * 20
    new_oid = "bb" * 20
    wrong_oid = "cc" * 20

    # Create
    @conn.exec_params("SELECT git_ref_update($1, 'refs/heads/main', decode($2, 'hex'))", [@repo_id, old_oid])

    # Update with wrong old value
    result = @conn.exec_params(
      "SELECT git_ref_update($1, 'refs/heads/main', decode($2, 'hex'), decode($3, 'hex'))",
      [@repo_id, new_oid, wrong_oid]
    )
    assert_equal "f", result[0]["git_ref_update"]
  end

  def test_force_update_ref
    old_oid = "aa" * 20
    new_oid = "bb" * 20
    wrong_oid = "cc" * 20

    @conn.exec_params("SELECT git_ref_update($1, 'refs/heads/main', decode($2, 'hex'))", [@repo_id, old_oid])

    # Force update with wrong old value
    result = @conn.exec_params(
      "SELECT git_ref_update($1, 'refs/heads/main', decode($2, 'hex'), decode($3, 'hex'), true)",
      [@repo_id, new_oid, wrong_oid]
    )
    assert_equal "t", result[0]["git_ref_update"]
  end

  def test_symbolic_ref
    @conn.exec_params("SELECT git_ref_set_symbolic($1, 'HEAD', 'refs/heads/main')", [@repo_id])

    result = @conn.exec_params(
      "SELECT symbolic FROM refs WHERE repo_id = $1 AND name = 'HEAD'",
      [@repo_id]
    )
    assert_equal "refs/heads/main", result[0]["symbolic"]
  end

  def test_delete_ref
    oid = "aa" * 20
    @conn.exec_params("SELECT git_ref_update($1, 'refs/heads/main', decode($2, 'hex'))", [@repo_id, oid])

    # Delete by setting to zero OID
    zero_oid = "00" * 20
    result = @conn.exec_params(
      "SELECT git_ref_update($1, 'refs/heads/main', decode($2, 'hex'), NULL, true)",
      [@repo_id, zero_oid]
    )
    assert_equal "t", result[0]["git_ref_update"]

    # Verify deleted
    result = @conn.exec_params(
      "SELECT count(*) FROM refs WHERE repo_id = $1 AND name = 'refs/heads/main'",
      [@repo_id]
    )
    assert_equal "0", result[0]["count"]
  end
end
